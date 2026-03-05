/*
 * cudf_engine_stub.c — No-op stub implementation of the cuDF engine C ABI.
 *
 * This stub compiles without any RAPIDS/cuDF dependency.  It is linked by
 * the Rust crate when CUDF_ENGINE_LIB is not set in the build environment,
 * allowing the Rust/Python code to compile and run on systems where RAPIDS
 * is not yet available (e.g. CUDA 13.1 before RAPIDS 25.06).
 *
 * Behaviour:
 *   cudf_engine_init()       → returns false  (engine not available)
 *   cudf_execute_substrait() → returns -ENOSYS, writes message to error_msg
 *   All other functions      → no-op / return NULL / return 0
 *
 * Phase 4 upgrade:
 *   Set CUDF_ENGINE_LIB=/path/to/dir containing libcudf_engine.so, then
 *   rebuild.  build.rs will skip this stub and link the real library.
 */

#include "cudf_engine.h"

#include <errno.h>
#include <stddef.h>
#include <string.h>

/* ENOSYS = 38 on Linux (function not implemented). */
#ifndef ENOSYS
#define ENOSYS 38
#endif

/* Suppress unused-function warning for the stub helper. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static void write_error(char* buf, size_t len, const char* msg) {
    if (buf && len > 0) {
        strncpy(buf, msg, len - 1);
        buf[len - 1] = '\0';
    }
}
#pragma GCC diagnostic pop

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes) {
    (void)cache_bytes;
    (void)proc_bytes;
    /* Stub: cuDF engine not available.  Return false so callers fall back. */
    return false;
}

void cudf_engine_destroy(void) {
    /* no-op */
}

/* ── Data loading ────────────────────────────────────────────────────────── */

GpuTableHandle* cudf_load_parquet(
    const char*  path,
    const char** column_names,
    int          num_columns)
{
    (void)path;
    (void)column_names;
    (void)num_columns;
    return NULL;
}

GpuTableHandle* cudf_import_arrow(
    ArrowArray*  array,
    ArrowSchema* schema)
{
    (void)array;
    (void)schema;
    return NULL;
}

void cudf_table_free(GpuTableHandle* handle) {
    (void)handle;
    /* no-op */
}

/* ── Core execution ──────────────────────────────────────────────────────── */

int cudf_execute_substrait(
    const uint8_t*   plan_bytes,
    size_t           plan_len,
    const char**     table_names,
    GpuTableHandle** table_handles,
    int              num_tables,
    ArrowArray*      out_array,
    ArrowSchema*     out_schema,
    char*            error_msg,
    size_t           error_msg_len)
{
    (void)plan_bytes;
    (void)plan_len;
    (void)table_names;
    (void)table_handles;
    (void)num_tables;
    (void)out_array;
    (void)out_schema;
    write_error(error_msg, error_msg_len,
                "cudf_engine_stub: C++ libcudf engine not available "
                "(requires RAPIDS 25.06+ with CUDA 13.x support). "
                "Set CUDF_ENGINE_LIB to link the real engine.");
    return -ENOSYS;
}

/* ── Availability probe ──────────────────────────────────────────────────── */

bool cudf_engine_is_ready(void) { return false; }

/* ── Memory diagnostics ──────────────────────────────────────────────────── */

size_t cudf_cache_used_bytes(void)  { return 0; }
size_t cudf_cache_total_bytes(void) { return 0; }
