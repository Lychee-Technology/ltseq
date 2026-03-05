/*
 * cudf_engine.h — Stable C ABI for the libcudf GPU execution engine.
 *
 * All exported symbols use plain C linkage to guarantee ABI stability
 * across compiler versions and Rust/C++ build toolchains.
 *
 * Phase 4 upgrade path:
 *   Replace cudf_engine_stub.c with a real libcudf implementation.
 *   The ABI defined here must remain unchanged.
 *
 * Thread safety: all functions are safe to call from multiple threads
 * provided cudf_engine_init() has completed before any other call.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Symbol visibility: all cudf_engine_* symbols must be exported */
#if defined(__GNUC__) || defined(__clang__)
#  define CUDF_ENGINE_API __attribute__((visibility("default")))
#else
#  define CUDF_ENGINE_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque types ────────────────────────────────────────────────────────── */

/**
 * Opaque handle to a table resident in GPU HBM.
 * Created by cudf_load_parquet() or cudf_import_arrow().
 * Must be freed with cudf_table_free() when no longer needed.
 */
typedef struct GpuTableHandle GpuTableHandle;

/**
 * Arrow C Data Interface — vendored struct definitions so callers do not
 * need to install Arrow headers.  Layout follows the Apache Arrow C Data
 * Interface specification exactly.
 */
#include "arrow_c_abi.h"

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * Initialize RMM memory pools and the cuDF execution engine.
 *
 * @param cache_bytes  GPU cache region size (persistent table data).
 * @param proc_bytes   GPU processing region size (intermediate results).
 * @return true on success, false if GPU is unavailable or already initialised.
 *
 * Call once at process start.  Subsequent calls are no-ops and return false.
 */
CUDF_ENGINE_API bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes);

/**
 * Release all GPU resources held by the engine.
 * Must not be called concurrently with any other cudf_* function.
 */
CUDF_ENGINE_API void cudf_engine_destroy(void);

/* ── Data loading (one-time cost; data stays in GPU HBM) ────────────────── */

/**
 * Load a Parquet file directly into GPU HBM using libcudf's native reader.
 * Avoids any CPU→GPU transfer of the data columns.
 *
 * @param path          Path to the Parquet file (UTF-8, null-terminated).
 * @param column_names  NULL-terminated array of column name strings to load.
 *                      Pass NULL to load all columns.
 * @param num_columns   Length of column_names (0 if column_names is NULL).
 * @return Opaque table handle, or NULL on error.
 *
 * Caller is responsible for calling cudf_table_free() on the returned handle.
 */
CUDF_ENGINE_API GpuTableHandle* cudf_load_parquet(
    const char*  path,
    const char** column_names,
    int          num_columns
);

/**
 * Import an Arrow table from CPU memory into GPU HBM via PCIe transfer.
 * Prefer cudf_load_parquet() on the hot path; this is for cold-start only.
 *
 * @param array   Pointer to an Arrow C Data Interface ArrowArray struct.
 * @param schema  Pointer to the corresponding ArrowSchema struct.
 * @return Opaque table handle, or NULL on error.
 *
 * Ownership of array/schema is NOT transferred; caller retains them.
 * Caller is responsible for calling cudf_table_free() on the returned handle.
 */
CUDF_ENGINE_API GpuTableHandle* cudf_import_arrow(
    ArrowArray*  array,
    ArrowSchema* schema
);

/**
 * Free a GPU table handle and release its HBM allocation back to the pool.
 * Passing NULL is a no-op.
 */
CUDF_ENGINE_API void cudf_table_free(GpuTableHandle* handle);

/* ── Core execution ──────────────────────────────────────────────────────── */

/**
 * Execute a Substrait plan on the GPU.
 *
 * The plan is decoded by the C++ SubstraitPlanner, dispatched through the
 * push-based GpuPipeline, and executed entirely on the GPU using libcudf
 * operators.  Only the final (small) result is transferred back to the CPU
 * via the Arrow C Data Interface — this is the only PCIe transfer.
 *
 * @param plan_bytes    Serialised substrait::Plan protobuf.
 * @param plan_len      Byte length of plan_bytes.
 * @param table_names   Array of table name strings referenced by the plan.
 * @param table_handles Array of GPU table handles, one per table_names entry.
 * @param num_tables    Length of the two arrays above.
 * @param out_array     Output: populated ArrowArray (caller must release).
 * @param out_schema    Output: populated ArrowSchema (caller must release).
 * @param error_msg     Output buffer for a human-readable error string.
 * @param error_msg_len Capacity of error_msg in bytes.
 * @return 0 on success; negative POSIX error code on failure
 *         (-ENOSYS if the engine is not initialised or unavailable).
 */
CUDF_ENGINE_API int cudf_execute_substrait(
    const uint8_t*   plan_bytes,
    size_t           plan_len,
    const char**     table_names,
    GpuTableHandle** table_handles,
    int              num_tables,
    ArrowArray*      out_array,
    ArrowSchema*     out_schema,
    char*            error_msg,
    size_t           error_msg_len
);

/* ── Availability probe ──────────────────────────────────────────────────── */

/**
 * Returns true if the engine has been successfully initialized.
 * Safe to call at any time; does not modify engine state.
 */
CUDF_ENGINE_API bool cudf_engine_is_ready(void);

/* ── Memory diagnostics ──────────────────────────────────────────────────── */

/** Bytes currently occupied in the GPU cache region. */
CUDF_ENGINE_API size_t cudf_cache_used_bytes(void);

/** Total capacity of the GPU cache region. */
CUDF_ENGINE_API size_t cudf_cache_total_bytes(void);

#ifdef __cplusplus
}
#endif
