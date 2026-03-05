/*
 * cudf_engine.cpp — Real libcudf implementation of the cudf_engine C ABI.
 *
 * Phase 4: replaces cudf_engine_stub.c with a real implementation using
 * RAPIDS libcudf 26.02.  Data is loaded directly into GPU HBM via
 * cudf::io::read_parquet() and never touches the CPU during query execution.
 * Only the final (small) result is transferred back via the Arrow C Data
 * Interface.
 *
 * Supported operations:
 *   cudf_load_parquet   — native GPU Parquet reader
 *   cudf_execute_substrait — scan + filter (simple column comparisons)
 *   cudf_table_free     — returns GPU memory to RMM pool
 */

#include "cudf_engine.h"
#include "substrait_planner.hpp"

#include <cudf/binaryop.hpp>
#include <cudf/copying.hpp>
#include <cudf/interop.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <rmm/mr/cuda_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>
#include <rmm/mr/pool_memory_resource.hpp>

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// ── Internal GpuTableHandle struct ───────────────────────────────────────────

struct GpuTableHandle {
    std::unique_ptr<cudf::table> table;
    std::vector<std::string> column_names;

    cudf::table_view view() const { return table->view(); }

    // Find column index by name (-1 if not found)
    int col_index(const std::string& name) const {
        for (int i = 0; i < static_cast<int>(column_names.size()); ++i) {
            if (column_names[i] == name) return i;
        }
        return -1;
    }
};

// ── Engine global state ───────────────────────────────────────────────────────

namespace {

std::once_flag g_init_flag;
std::atomic<bool> g_initialized{false};

// RMM memory resources (lifetime: process)
rmm::mr::cuda_memory_resource g_cuda_mr;
std::unique_ptr<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>> g_pool_mr;

void write_error(char* buf, size_t len, const char* msg) {
    if (buf && len > 0) {
        std::strncpy(buf, msg, len - 1);
        buf[len - 1] = '\0';
    }
}

}  // namespace

// ── Lifecycle ─────────────────────────────────────────────────────────────────

bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes) {
    // Idempotent: return false on second call (already initialised).
    bool already = false;
    if (g_initialized.compare_exchange_strong(already, true)) {
        try {
            // Use pool_memory_resource for fast GPU allocations.
            // initial_pool_size = max(cache_bytes + proc_bytes, 256 MiB)
            size_t total = cache_bytes + proc_bytes;
            if (total < 256UL * 1024 * 1024) total = 256UL * 1024 * 1024;

            g_pool_mr = std::make_unique<
                rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>>(
                &g_cuda_mr,
                /*initial_pool_size=*/total,
                /*maximum_pool_size=*/std::optional<std::size_t>{});

            rmm::mr::set_current_device_resource(g_pool_mr.get());
            return true;
        } catch (std::exception const& e) {
            // GPU unavailable — reset flag so callers see false
            g_initialized.store(false);
            return false;
        }
    }
    return false;  // Already initialised by a previous call
}

void cudf_engine_destroy() {
    if (g_initialized.exchange(false)) {
        rmm::mr::set_current_device_resource(nullptr);
        g_pool_mr.reset();
    }
}

// ── Data loading ──────────────────────────────────────────────────────────────

GpuTableHandle* cudf_load_parquet(
    const char*  path,
    const char** column_names,
    int          num_columns)
{
    if (!path) return nullptr;
    try {
        auto source = cudf::io::source_info{std::string(path)};
        auto builder = cudf::io::parquet_reader_options::builder(source);

        if (num_columns > 0 && column_names) {
            std::vector<std::string> cols;
            for (int i = 0; i < num_columns; ++i) {
                if (column_names[i]) cols.emplace_back(column_names[i]);
            }
            builder.columns(cols);
        }

        auto result = cudf::io::read_parquet(builder.build());

        auto* handle = new GpuTableHandle{};
        handle->table = std::move(result.tbl);
        for (const auto& ci : result.metadata.schema_info) {
            handle->column_names.push_back(ci.name);
        }
        return handle;
    } catch (...) {
        return nullptr;
    }
}

GpuTableHandle* cudf_import_arrow(ArrowArray* array, ArrowSchema* schema) {
    if (!array || !schema) return nullptr;
    try {
        auto tbl = cudf::from_arrow(
            reinterpret_cast<ArrowSchema const*>(schema),
            reinterpret_cast<ArrowArray const*>(array));

        // Collect column names from ArrowSchema
        auto* handle = new GpuTableHandle{};
        handle->table = std::move(tbl);
        for (int i = 0; i < static_cast<int>(schema->n_children); ++i) {
            handle->column_names.emplace_back(schema->children[i]->name
                                              ? schema->children[i]->name : "");
        }
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void cudf_table_free(GpuTableHandle* handle) {
    delete handle;
}

// ── Core execution ────────────────────────────────────────────────────────────

// Apply a scalar filter predicate to a table; returns the filtered table.
static std::unique_ptr<cudf::table> apply_filter(
    const GpuTableHandle* handle,
    const FilterPred& pred)
{
    int col_idx = handle->col_index(pred.column_name);
    if (col_idx < 0) {
        throw std::runtime_error("Column not found: " + pred.column_name);
    }

    auto view = handle->view();
    const auto& col = view.column(col_idx);

    // Build the scalar threshold
    std::unique_ptr<cudf::scalar> scalar_val;
    cudf::binary_operator bop;

    switch (pred.op) {
        case CompareOp::GT:  bop = cudf::binary_operator::GREATER;          break;
        case CompareOp::GTE: bop = cudf::binary_operator::GREATER_EQUAL;    break;
        case CompareOp::LT:  bop = cudf::binary_operator::LESS;             break;
        case CompareOp::LTE: bop = cudf::binary_operator::LESS_EQUAL;       break;
        case CompareOp::EQ:  bop = cudf::binary_operator::EQUAL;            break;
        case CompareOp::NEQ: bop = cudf::binary_operator::NOT_EQUAL;        break;
    }

    // Create scalar from threshold variant
    if (std::holds_alternative<int64_t>(pred.threshold)) {
        int64_t val = std::get<int64_t>(pred.threshold);
        if (col.type().id() == cudf::type_id::INT32) {
            scalar_val = cudf::make_numeric_scalar(cudf::data_type{cudf::type_id::INT32});
            static_cast<cudf::numeric_scalar<int32_t>*>(scalar_val.get())->set_value(
                static_cast<int32_t>(val));
        } else {
            scalar_val = cudf::make_numeric_scalar(cudf::data_type{cudf::type_id::INT64});
            static_cast<cudf::numeric_scalar<int64_t>*>(scalar_val.get())->set_value(val);
        }
    } else {
        double val = std::get<double>(pred.threshold);
        if (col.type().id() == cudf::type_id::FLOAT32) {
            scalar_val = cudf::make_numeric_scalar(cudf::data_type{cudf::type_id::FLOAT32});
            static_cast<cudf::numeric_scalar<float>*>(scalar_val.get())->set_value(
                static_cast<float>(val));
        } else {
            scalar_val = cudf::make_numeric_scalar(cudf::data_type{cudf::type_id::FLOAT64});
            static_cast<cudf::numeric_scalar<double>*>(scalar_val.get())->set_value(val);
        }
    }

    // Compute boolean mask: col <op> scalar
    auto mask_col = cudf::binary_operation(
        col, *scalar_val, bop, cudf::data_type{cudf::type_id::BOOL8});

    // Apply mask to the full table
    return cudf::apply_boolean_mask(view, mask_col->view());
}

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
    if (!plan_bytes || !out_array || !out_schema) {
        write_error(error_msg, error_msg_len, "cudf_execute_substrait: null argument");
        return -EINVAL;
    }

    // 1. Decode the Substrait plan
    DecodedPlan decoded = decode_substrait(plan_bytes, plan_len, table_names, num_tables);
    if (std::getenv("LTSEQ_GPU_DEBUG")) {
        fprintf(stderr, "[cudf] decoded: ok=%d table='%s' local_file='%s' filter=%s project_cols=%zu\n",
                decoded.ok, decoded.table_name.c_str(), decoded.local_file_path.c_str(),
                decoded.filter.has_value() ? "YES" : "NO",
                decoded.project_columns.size());
        if (decoded.filter.has_value()) {
            const auto& f = *decoded.filter;
            auto op_str = [](CompareOp op) -> const char* {
                switch(op) { case CompareOp::GT: return "GT"; case CompareOp::GTE: return "GTE";
                             case CompareOp::LT: return "LT"; case CompareOp::LTE: return "LTE";
                             case CompareOp::EQ: return "EQ"; case CompareOp::NEQ: return "NEQ"; }
                return "?";
            };
            if (std::holds_alternative<int64_t>(f.threshold))
                fprintf(stderr, "[cudf] filter: col='%s' op=%s threshold_i64=%lld\n",
                        f.column_name.c_str(), op_str(f.op),
                        (long long)std::get<int64_t>(f.threshold));
            else
                fprintf(stderr, "[cudf] filter: col='%s' op=%s threshold_f64=%f\n",
                        f.column_name.c_str(), op_str(f.op),
                        std::get<double>(f.threshold));
        }
        if (!decoded.error_msg.empty())
            fprintf(stderr, "[cudf] decode error: %s\n", decoded.error_msg.c_str());
    }
    if (!decoded.ok) {
        write_error(error_msg, error_msg_len,
                    ("Substrait decode failed: " + decoded.error_msg).c_str());
        return -ENOSYS;
    }

    // 2. Find or auto-load the table handle
    GpuTableHandle* src_handle = nullptr;
    std::unique_ptr<GpuTableHandle> auto_handle;  // owns if auto-loaded

    if (!decoded.local_file_path.empty()) {
        // Auto-load mode: load directly from the Parquet file in the plan
        auto_handle.reset(cudf_load_parquet(decoded.local_file_path.c_str(), nullptr, 0));
        if (!auto_handle) {
            write_error(error_msg, error_msg_len,
                        ("cudf_load_parquet failed for: " + decoded.local_file_path).c_str());
            return -EIO;
        }
        src_handle = auto_handle.get();
    } else {
        // Named-table mode: look up the caller-provided handle by table name
        for (int i = 0; i < num_tables; ++i) {
            if (table_names[i] && decoded.table_name == table_names[i]) {
                src_handle = table_handles[i];
                break;
            }
        }
        // If not found by name and only one table, use it
        if (!src_handle && num_tables == 1) {
            src_handle = table_handles[0];
        }
        if (!src_handle) {
            write_error(error_msg, error_msg_len,
                        ("Table not found: " + decoded.table_name).c_str());
            return -ENOENT;
        }
    }

    try {
        // 3. Execute operations: Filter
        std::unique_ptr<cudf::table> result_table;
        const GpuTableHandle* working_handle = src_handle;
        std::unique_ptr<GpuTableHandle> filtered_handle;  // keeps temp result alive

        if (decoded.filter) {
            result_table = apply_filter(src_handle, *decoded.filter);
            filtered_handle = std::make_unique<GpuTableHandle>();
            filtered_handle->table = std::move(result_table);
            filtered_handle->column_names = src_handle->column_names;
            working_handle = filtered_handle.get();
        }

        cudf::table_view result_view = working_handle->view();

        // 4. Build Arrow column metadata for schema export
        std::vector<cudf::column_metadata> col_meta;
        for (const auto& name : working_handle->column_names) {
            col_meta.emplace_back(name);
        }

        // 5. Export schema
        auto schema_uptr = cudf::to_arrow_schema(result_view, col_meta);
        *out_schema = *schema_uptr;
        schema_uptr->release = nullptr;  // Transfer ownership to caller

        // 6. D2H transfer: GPU → CPU via Arrow C Data Interface
        auto arr_uptr = cudf::to_arrow_host(result_view);
        *out_array = arr_uptr->array;
        arr_uptr->array.release = nullptr;  // Transfer ownership to caller

        return 0;
    } catch (std::exception const& e) {
        write_error(error_msg, error_msg_len, e.what());
        return -EIO;
    } catch (...) {
        write_error(error_msg, error_msg_len, "Unknown exception in cudf_execute_substrait");
        return -EIO;
    }
}

// ── Availability probe ────────────────────────────────────────────────────────

bool cudf_engine_is_ready() {
    return g_initialized.load(std::memory_order_relaxed);
}

// ── Memory diagnostics ────────────────────────────────────────────────────────

size_t cudf_cache_used_bytes() {
    if (!g_pool_mr) return 0;
    return 0;  // pool_memory_resource doesn't expose used bytes directly
}

size_t cudf_cache_total_bytes() {
    if (!g_pool_mr) return 0;
    return 0;
}
