//! Rust FFI bindings for the C++ libcudf execution engine.
// Phase 4 forward-looking APIs: suppress dead_code until the real engine lands.
#![allow(dead_code)]
//!
//! # Architecture
//!
//! The C++ engine exposes a stable C ABI defined in `cpp/include/cudf_engine.h`.
//! This module provides:
//!
//! 1. **Raw `extern "C"` declarations** — unsafe, one-to-one mapping of the
//!    C header.
//! 2. **Safe Rust wrappers** — [`GpuTableHandle`] (RAII drop), and the
//!    [`cudf_engine_available`] probe used by [`GpuExecNode`].
//!
//! # Phase 4 upgrade path
//!
//! Currently the crate links against `cudf_engine_stub.c` which returns
//! `false`/`-ENOSYS` for every call.  When RAPIDS 25.06+ becomes available:
//!
//! 1. Build `libcudf_engine.so` via `cmake -S cpp -B build/cpp && cmake --build …`
//! 2. Set `CUDF_ENGINE_LIB=/path/to/dir` and rebuild the crate.
//! 3. `cudf_engine_available()` will return `true`; `GpuExecNode::execute()`
//!    will route to [`execute_substrait`] instead of the CPU fallback.

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr::NonNull;

use arrow::array::StructArray;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

// ── Raw extern "C" declarations ──────────────────────────────────────────────

/// Opaque C++ object.  Never dereferenced from Rust.
#[repr(C)]
pub struct GpuTableHandleOpaque {
    _private: [u8; 0],
}

#[link(name = "cudf_engine")]
unsafe extern "C" {
    fn cudf_engine_init(cache_bytes: usize, proc_bytes: usize) -> bool;
    fn cudf_engine_destroy();

    fn cudf_load_parquet(
        path: *const c_char,
        column_names: *const *const c_char,
        num_columns: c_int,
    ) -> *mut GpuTableHandleOpaque;

    fn cudf_table_free(handle: *mut GpuTableHandleOpaque);

    fn cudf_execute_substrait(
        plan_bytes: *const u8,
        plan_len: usize,
        table_names: *const *const c_char,
        table_handles: *const *mut GpuTableHandleOpaque,
        num_tables: c_int,
        out_array: *mut FFI_ArrowArray,
        out_schema: *mut FFI_ArrowSchema,
        error_msg: *mut c_char,
        error_msg_len: usize,
    ) -> c_int;

    fn cudf_engine_is_ready() -> bool;
    fn cudf_cache_used_bytes() -> usize;
    fn cudf_cache_total_bytes() -> usize;
}

// ── Availability probe ───────────────────────────────────────────────────────

/// Initialize the C++ cuDF engine if not already done, and return whether
/// it is available.  Safe to call multiple times (idempotent after first).
pub fn cudf_engine_startup(cache_bytes: usize, proc_bytes: usize) -> bool {
    // First call initializes; subsequent calls are no-ops in C++.
    unsafe { cudf_engine_init(cache_bytes, proc_bytes) };
    unsafe { cudf_engine_is_ready() }
}

/// Return `true` if the C++ cuDF engine has been successfully initialised.
/// Does not modify any state; safe to call from multiple threads.
pub fn cudf_engine_available() -> bool {
    unsafe { cudf_engine_is_ready() }
}

/// Memory diagnostics — bytes currently used in the GPU cache region.
pub fn cache_used_bytes() -> usize {
    unsafe { cudf_cache_used_bytes() }
}

/// Memory diagnostics — total capacity of the GPU cache region.
pub fn cache_total_bytes() -> usize {
    unsafe { cudf_cache_total_bytes() }
}

// ── Safe RAII handle ─────────────────────────────────────────────────────────

/// Owned handle to a table resident in GPU HBM.
///
/// Dropping this value calls `cudf_table_free()` and returns the GPU memory
/// to the RMM pool.
pub struct GpuTableHandle {
    ptr: NonNull<GpuTableHandleOpaque>,
}

impl Drop for GpuTableHandle {
    fn drop(&mut self) {
        unsafe { cudf_table_free(self.ptr.as_ptr()) }
    }
}

// SAFETY: the C++ engine manages its own locking; handles are safe to send
// across threads and to share read-only across concurrent queries.
unsafe impl Send for GpuTableHandle {}
unsafe impl Sync for GpuTableHandle {}

impl GpuTableHandle {
    /// Load a Parquet file into GPU HBM using libcudf's native reader.
    ///
    /// Returns `None` if the engine is unavailable or the load fails.
    pub fn load_parquet(path: &str, columns: &[&str]) -> Option<Self> {
        let c_path = CString::new(path).ok()?;
        let c_cols: Vec<CString> = columns
            .iter()
            .filter_map(|s| CString::new(*s).ok())
            .collect();
        let ptrs: Vec<*const c_char> = c_cols.iter().map(|s| s.as_ptr()).collect();

        let ptr = unsafe {
            cudf_load_parquet(c_path.as_ptr(), ptrs.as_ptr(), columns.len() as c_int)
        };

        NonNull::new(ptr).map(|ptr| Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *mut GpuTableHandleOpaque {
        self.ptr.as_ptr()
    }
}

// ── Safe execute_substrait wrapper ───────────────────────────────────────────

/// Execute a Substrait plan on the GPU and return the result as a
/// CPU-side Arrow [`RecordBatch`].
///
/// This is the only point where data crosses the PCIe bus; the result
/// (e.g. 365 aggregated rows after a full TPC-H Q1) is typically tiny.
///
/// # Errors
///
/// Returns an error if the C++ engine returns a non-zero exit code
/// (including `-ENOSYS` from the stub).
pub fn execute_substrait(
    plan_bytes: &[u8],
    tables: &[(&str, &GpuTableHandle)],
) -> Result<RecordBatch> {
    let c_names: Vec<CString> = tables
        .iter()
        .map(|(name, _)| CString::new(*name).expect("table name contains nul"))
        .collect();
    let name_ptrs: Vec<*const c_char> = c_names.iter().map(|s| s.as_ptr()).collect();
    let handle_ptrs: Vec<*mut GpuTableHandleOpaque> =
        tables.iter().map(|(_, h)| h.as_ptr()).collect();

    let mut out_array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    let mut error_buf = vec![0i8; 1024];

    let ret = unsafe {
        cudf_execute_substrait(
            plan_bytes.as_ptr(),
            plan_bytes.len(),
            name_ptrs.as_ptr(),
            handle_ptrs.as_ptr(),
            tables.len() as c_int,
            &mut out_array,
            &mut out_schema,
            error_buf.as_mut_ptr(),
            error_buf.len(),
        )
    };

    if ret != 0 {
        let msg = unsafe { CStr::from_ptr(error_buf.as_ptr()) }
            .to_string_lossy()
            .to_string();
        return Err(DataFusionError::External(msg.into()));
    }

    // Arrow C Data Interface → Rust RecordBatch.
    let array_data = unsafe { from_ffi(out_array, &out_schema) }
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let struct_array = StructArray::from(array_data);
    Ok(RecordBatch::from(&struct_array))
}
