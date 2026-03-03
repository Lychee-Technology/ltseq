//! GPU filter operations on raw device pointers from external sources (e.g., cuDF).
//!
//! This module enables the B-extended architecture:
//!   cuDF reads Parquet → GPU VRAM (no CPU decode)
//!   → our CUDA kernels run directly on cuDF's device pointers (no H2D!)
//!   → mask stays in GPU memory
//!   → Python applies mask via cupy (no D2H for intermediate results)
//!   → single D2H only at `.to_ltseq()` materialization
//!
//! The key trick: `CudaStream::upgrade_device_ptr()` wraps an external raw CUDA
//! device pointer into a `CudaSlice` without allocating or copying. We then pass
//! it to the existing `launch_builder`, which pushes the device pointer address
//! as a kernel argument — exactly as it would for any owned `CudaSlice`. After
//! the kernel is launched, `leak()` prevents cudarc from freeing cuDF's memory.

use cudarc::driver::safe::CudaSlice;
use cudarc::driver::safe::LaunchConfig;
use cudarc::driver::PushKernelArg;

use super::{get_context, get_filter_function, get_stream, CUDA_BLOCK_SIZE, grid_size};

/// Run a CUDA filter kernel on a raw GPU device pointer from cuDF.
///
/// Takes a raw CUDA device pointer (from `cudf_series.data.ptr`), allocates a
/// `u8` mask in GPU memory, runs the filter kernel **without any H2D transfer**,
/// and returns the mask's device pointer. The caller must free the mask via
/// [`gpu_free_ptr_impl`] after applying it.
///
/// # Arguments
/// - `col_ptr`  : Raw CUDA device pointer (Python `int`) — cuDF column data
/// - `n`        : Number of elements in the column
/// - `dtype`    : `"i32"`, `"i64"`, `"f32"`, or `"f64"`
/// - `op`       : `"gt"`, `"lt"`, `"gte"`, `"lte"`, `"eq"`, `"neq"`
/// - `threshold`: Comparison scalar (converted to column dtype internally)
///
/// # Returns
/// Raw CUDA device pointer to a `u8` mask of length `n` (1=keep, 0=drop).
///
/// # Safety contract (caller)
/// - `col_ptr` is a valid allocation on CUDA device 0 with ≥ `n` elements of `dtype`
/// - The allocation is kept alive by the caller (e.g., cuDF DataFrame) until this
///   function returns and the mask has been applied
pub fn gpu_filter_raw_impl(
    col_ptr: u64,
    n: usize,
    dtype: &str,
    op: &str,
    threshold: f64,
) -> pyo3::PyResult<u64> {
    let stream = get_stream().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("GPU not available (no CUDA device found)")
    })?;
    let ctx = get_context().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("GPU context unavailable")
    })?;

    let kernel_name = format!("filter_{}_{}", op, dtype);
    let func = get_filter_function(&kernel_name).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown GPU kernel '{}' (dtype='{}', op='{}'). \
             dtype must be i32/i64/f32/f64, op must be gt/lt/gte/lte/eq/neq.",
            kernel_name, dtype, op
        ))
    })?;

    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    // Allocate output mask in GPU memory (owned by cudarc).
    // This allocation will be leaked to Python; caller frees via gpu_free_ptr().
    let mut d_mask = stream.alloc_zeros::<u8>(n).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("GPU mask alloc failed: {:?}", e))
    })?;

    // Wrap the external device pointer in a CudaSlice without allocating.
    //
    // We disable event tracking before calling upgrade_device_ptr so the
    // resulting CudaSlice has read=None, write=None. This is necessary because:
    //   - upgrade_device_ptr normally creates fresh (unrecorded) CudaEvents
    //   - leak() calls stream.wait(event) on these, which can deadlock on an
    //     unrecorded event
    //   - With events=None, leak() skips all stream.wait() calls — safe and instant
    //
    // The Python GIL ensures no concurrent Python-side GPU access during this call.
    unsafe { ctx.disable_event_tracking() };
    let d_input: CudaSlice<u8> = unsafe { stream.upgrade_device_ptr(col_ptr, n) };
    unsafe { ctx.enable_event_tracking() };

    // Launch kernel with the external device pointer as input.
    // Since d_input.read=None and d_input.write=None, PushKernelArg skips all
    // event waits/records (is_managing_stream_synchronization() is also false on
    // the default/null stream). The kernel receives &d_input.cu_device_ptr = col_ptr.
    let launch_result = match dtype {
        "i64" => {
            let thr = threshold as i64;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_input)
                    .arg(&thr)
                    .arg(&mut d_mask)
                    .arg(&n_u32)
                    .launch(launch_cfg)
            }
        }
        "i32" => {
            let thr = threshold as i32;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_input)
                    .arg(&thr)
                    .arg(&mut d_mask)
                    .arg(&n_u32)
                    .launch(launch_cfg)
            }
        }
        "f64" => unsafe {
            stream
                .launch_builder(&func)
                .arg(&d_input)
                .arg(&threshold)
                .arg(&mut d_mask)
                .arg(&n_u32)
                .launch(launch_cfg)
        },
        "f32" => {
            let thr = threshold as f32;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_input)
                    .arg(&thr)
                    .arg(&mut d_mask)
                    .arg(&n_u32)
                    .launch(launch_cfg)
            }
        }
        other => {
            // Prevent freeing cuDF's memory before returning the error
            let _ = d_input.leak();
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unsupported dtype '{}'. Must be one of: i32, i64, f32, f64.",
                other
            )));
        }
    };

    // Prevent cudarc from freeing cuDF's GPU memory — cuDF owns it.
    // d_input.read and d_input.write are None (event tracking was disabled),
    // so leak() is instant: no stream.wait() calls, no CUDA synchronization.
    let _ = d_input.leak();

    launch_result.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("CUDA kernel launch failed: {:?}", e))
    })?;

    // Synchronize so the mask is visible when cupy reads it on the Python side.
    // Both our kernels and cupy use the default (null) CUDA stream, so this is
    // a lightweight CPU-side flush to ensure the kernel is complete.
    stream.synchronize().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("CUDA sync failed: {:?}", e))
    })?;

    // Transfer ownership of the mask to Python. Python calls gpu_free_ptr() to free it.
    let mask_ptr = d_mask.leak() as u64;
    Ok(mask_ptr)
}

/// Free a GPU mask buffer previously allocated by [`gpu_filter_raw_impl`].
///
/// Must be called after the mask has been consumed by cupy/cuDF on the Python side.
///
/// # Arguments
/// - `ptr`: Device pointer returned by `gpu_filter_raw_impl`
/// - `n`  : Element count passed to `gpu_filter_raw_impl` (same `n`)
pub fn gpu_free_ptr_impl(ptr: u64, n: usize) -> pyo3::PyResult<()> {
    let stream = get_stream().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("GPU not available")
    })?;
    let ctx = get_context().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("GPU context unavailable")
    })?;

    // Wrap the raw pointer in a CudaSlice so its Drop impl frees the GPU memory.
    // Disable event tracking so the CudaSlice has no events (events=None),
    // making Drop skip stream.wait() and free immediately.
    unsafe { ctx.disable_event_tracking() };
    let _slice: CudaSlice<u8> = unsafe { stream.upgrade_device_ptr(ptr, n) };
    unsafe { ctx.enable_event_tracking() };

    // _slice drops here → calls result::free_async / result::free_sync on `ptr`
    Ok(())
}
