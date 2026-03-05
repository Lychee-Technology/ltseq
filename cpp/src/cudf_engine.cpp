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
 *   cudf_execute_substrait — scan + filter + aggregate + sort + join + distinct
 *   cudf_table_free     — returns GPU memory to RMM pool
 */

#include "cudf_engine.h"
#include "substrait_planner.hpp"

#include <cudf/aggregation.hpp>
#include <cudf/binaryop.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/copying.hpp>
#include <cudf/groupby.hpp>
#include <cudf/interop.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/join/join.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/sorting.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
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

// ── Hash Aggregate ───────────────────────────────────────────────────────────

/// Create a cudf groupby_aggregation from our AggFunc enum.
static std::unique_ptr<cudf::groupby_aggregation> make_cudf_agg(AggFunc func) {
    switch (func) {
        case AggFunc::SUM:       return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
        case AggFunc::MIN:       return cudf::make_min_aggregation<cudf::groupby_aggregation>();
        case AggFunc::MAX:       return cudf::make_max_aggregation<cudf::groupby_aggregation>();
        case AggFunc::COUNT:     return cudf::make_count_aggregation<cudf::groupby_aggregation>(
                                     cudf::null_policy::EXCLUDE);
        case AggFunc::COUNT_ALL: return cudf::make_count_aggregation<cudf::groupby_aggregation>(
                                     cudf::null_policy::INCLUDE);
        case AggFunc::MEAN:
        case AggFunc::AVG:       return cudf::make_mean_aggregation<cudf::groupby_aggregation>();
    }
    throw std::runtime_error("Unknown AggFunc");
}

/// Apply GROUP BY aggregate to a table.
/// Returns the result table and output column names.
static std::pair<std::unique_ptr<cudf::table>, std::vector<std::string>>
apply_hash_aggregate(
    cudf::table_view input,
    const std::vector<std::string>& input_col_names,
    const AggregateOp& agg_op)
{
    // Build the group-by keys table view
    std::vector<cudf::column_view> key_cols;
    for (int idx : agg_op.group_col_indices) {
        if (idx < 0 || idx >= input.num_columns())
            throw std::runtime_error("Aggregate group key index out of range");
        key_cols.push_back(input.column(idx));
    }
    cudf::table_view keys_view(key_cols);

    cudf::groupby::groupby gb(keys_view);

    // Build aggregation requests: one request per unique value column.
    // Multiple aggregations on the same column are grouped into one request.
    // We track the mapping so we can reassemble columns in the right order.
    struct RequestInfo {
        int request_idx;
        int agg_idx_in_request;
    };
    std::unordered_map<int, int> col_to_request;  // value_col_idx → request index
    std::vector<cudf::groupby::aggregation_request> requests;
    std::vector<RequestInfo> measure_mapping;  // one per measure

    for (size_t i = 0; i < agg_op.measures.size(); ++i) {
        const auto& m = agg_op.measures[i];
        int val_idx = m.value_col_idx;

        // For COUNT(*), use the first column as a dummy value column
        if (val_idx < 0) val_idx = 0;

        auto it = col_to_request.find(val_idx);
        if (it == col_to_request.end()) {
            // Create a new request for this value column
            cudf::groupby::aggregation_request req;
            req.values = input.column(val_idx);
            req.aggregations.push_back(make_cudf_agg(m.func));
            int req_idx = static_cast<int>(requests.size());
            col_to_request[val_idx] = req_idx;
            measure_mapping.push_back({req_idx, 0});
            requests.push_back(std::move(req));
        } else {
            // Add aggregation to existing request
            int req_idx = it->second;
            int agg_idx = static_cast<int>(requests[req_idx].aggregations.size());
            requests[req_idx].aggregations.push_back(make_cudf_agg(m.func));
            measure_mapping.push_back({req_idx, agg_idx});
        }
    }

    // Execute groupby aggregate
    auto [group_keys_tbl, agg_results] = gb.aggregate(requests);

    // Build the output table: [group_key_cols... | measure_result_cols...]
    std::vector<std::unique_ptr<cudf::column>> out_columns;
    std::vector<std::string> out_col_names;

    // Add group key columns (released from the keys table)
    auto key_columns = group_keys_tbl->release();
    for (size_t i = 0; i < key_columns.size(); ++i) {
        out_col_names.push_back(agg_op.group_col_names[i]);
        out_columns.push_back(std::move(key_columns[i]));
    }

    // Add measure result columns in the original order
    for (size_t i = 0; i < measure_mapping.size(); ++i) {
        const auto& info = measure_mapping[i];
        auto& result_col = agg_results[info.request_idx].results[info.agg_idx_in_request];

        const auto& m = agg_op.measures[i];
        std::string func_str;
        switch (m.func) {
            case AggFunc::SUM:       func_str = "SUM"; break;
            case AggFunc::MIN:       func_str = "MIN"; break;
            case AggFunc::MAX:       func_str = "MAX"; break;
            case AggFunc::COUNT:     func_str = "COUNT"; break;
            case AggFunc::COUNT_ALL: func_str = "COUNT"; break;
            case AggFunc::MEAN:
            case AggFunc::AVG:       func_str = "AVG"; break;
        }
        out_col_names.push_back(
            func_str + "(" + (m.value_col_name.empty() ? "*" : m.value_col_name) + ")");
        out_columns.push_back(std::move(result_col));
    }

    auto result_table = std::make_unique<cudf::table>(std::move(out_columns));
    return {std::move(result_table), std::move(out_col_names)};
}

// ── Hash Join ────────────────────────────────────────────────────────────────

/// Apply a hash join between left and right tables.
/// Returns the joined table and output column names.
static std::pair<std::unique_ptr<cudf::table>, std::vector<std::string>>
apply_hash_join(
    cudf::table_view left,
    const std::vector<std::string>& left_col_names,
    cudf::table_view right,
    const std::vector<std::string>& right_col_names,
    const JoinOp& join_op)
{
    // Build key table views
    std::vector<cudf::column_view> left_key_cols, right_key_cols;
    for (int idx : join_op.left_key_indices) {
        if (idx < 0 || idx >= left.num_columns())
            throw std::runtime_error("Join left key index out of range");
        left_key_cols.push_back(left.column(idx));
    }
    for (int idx : join_op.right_key_indices) {
        if (idx < 0 || idx >= right.num_columns())
            throw std::runtime_error("Join right key index out of range");
        right_key_cols.push_back(right.column(idx));
    }

    cudf::table_view left_keys(left_key_cols);
    cudf::table_view right_keys(right_key_cols);

    // Perform the join to get index pairs
    std::pair<std::unique_ptr<rmm::device_uvector<cudf::size_type>>,
              std::unique_ptr<rmm::device_uvector<cudf::size_type>>> indices;

    switch (join_op.type) {
        case JoinType::INNER:
            indices = cudf::inner_join(left_keys, right_keys);
            break;
        case JoinType::LEFT:
            indices = cudf::left_join(left_keys, right_keys);
            break;
        case JoinType::OUTER:
            indices = cudf::full_join(left_keys, right_keys);
            break;
        case JoinType::RIGHT:
            // Right join = swap sides + left join, then swap output columns
            indices = cudf::left_join(right_keys, left_keys);
            std::swap(indices.first, indices.second);
            break;
        default:
            throw std::runtime_error("Unsupported join type for GPU execution");
    }

    // Gather rows from both tables using the index pairs.
    // Create cudf::column from device_uvector<size_type> using the
    // template constructor for numeric types.
    auto left_indices_col = std::make_unique<cudf::column>(
        std::move(*indices.first),
        rmm::device_buffer{},  // null mask — no nulls in index column
        0);                    // null count
    auto right_indices_col = std::make_unique<cudf::column>(
        std::move(*indices.second),
        rmm::device_buffer{},
        0);

    auto left_result = cudf::gather(
        left, left_indices_col->view(),
        cudf::out_of_bounds_policy::NULLIFY);
    auto right_result = cudf::gather(
        right, right_indices_col->view(),
        cudf::out_of_bounds_policy::NULLIFY);

    // Concatenate left and right columns into a single output table
    std::vector<std::unique_ptr<cudf::column>> out_columns;
    std::vector<std::string> out_col_names;

    auto left_cols = left_result->release();
    for (size_t i = 0; i < left_cols.size(); ++i) {
        out_col_names.push_back(left_col_names[i]);
        out_columns.push_back(std::move(left_cols[i]));
    }
    auto right_cols = right_result->release();
    for (size_t i = 0; i < right_cols.size(); ++i) {
        out_col_names.push_back(right_col_names[i]);
        out_columns.push_back(std::move(right_cols[i]));
    }

    auto result_table = std::make_unique<cudf::table>(std::move(out_columns));
    return {std::move(result_table), std::move(out_col_names)};
}

// ── Sort ─────────────────────────────────────────────────────────────────────

/// Apply sort to a table by the specified sort keys.
/// Returns the sorted table (column names unchanged).
static std::unique_ptr<cudf::table> apply_sort(
    cudf::table_view input,
    const std::vector<std::string>& col_names,
    const SortOp& sort_op)
{
    // Build the sort key table and order/null-precedence vectors
    std::vector<cudf::column_view> key_cols;
    std::vector<cudf::order> column_order;
    std::vector<cudf::null_order> null_precedence;

    for (const auto& key : sort_op.keys) {
        if (key.col_idx < 0 || key.col_idx >= input.num_columns())
            throw std::runtime_error("Sort key index out of range");
        key_cols.push_back(input.column(key.col_idx));
        column_order.push_back(key.ascending ? cudf::order::ASCENDING
                                              : cudf::order::DESCENDING);
        null_precedence.push_back(key.nulls_first ? cudf::null_order::BEFORE
                                                   : cudf::null_order::AFTER);
    }

    cudf::table_view keys_view(key_cols);

    // Use sort_by_key: reorder all columns by the sort key columns
    return cudf::sort_by_key(input, keys_view, column_order, null_precedence);
}

// ── Distinct ─────────────────────────────────────────────────────────────────

/// Apply distinct (deduplication) to a table.
/// If key_col_indices is empty, all columns are used as keys.
static std::unique_ptr<cudf::table> apply_distinct(
    cudf::table_view input,
    const DistinctOp& distinct_op)
{
    std::vector<cudf::size_type> key_indices;
    if (distinct_op.key_col_indices.empty()) {
        // All columns are keys
        for (int i = 0; i < input.num_columns(); ++i) {
            key_indices.push_back(static_cast<cudf::size_type>(i));
        }
    } else {
        for (int idx : distinct_op.key_col_indices) {
            key_indices.push_back(static_cast<cudf::size_type>(idx));
        }
    }

    return cudf::stable_distinct(
        input, key_indices,
        cudf::duplicate_keep_option::KEEP_FIRST,
        cudf::null_equality::EQUAL,
        cudf::nan_equality::ALL_EQUAL);
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
        fprintf(stderr, "[cudf] decoded: ok=%d table='%s' local_file='%s' filter=%s agg=%s sort=%s join=%s distinct=%s project_cols=%zu\n",
                decoded.ok, decoded.table_name.c_str(), decoded.local_file_path.c_str(),
                decoded.filter.has_value() ? "YES" : "NO",
                decoded.aggregate.has_value() ? "YES" : "NO",
                decoded.sort.has_value() ? "YES" : "NO",
                decoded.join.has_value() ? "YES" : "NO",
                decoded.distinct.has_value() ? "YES" : "NO",
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
        if (decoded.aggregate.has_value()) {
            const auto& a = *decoded.aggregate;
            fprintf(stderr, "[cudf] aggregate: %zu group keys, %zu measures\n",
                    a.group_col_indices.size(), a.measures.size());
        }
        if (decoded.sort.has_value()) {
            fprintf(stderr, "[cudf] sort: %zu keys\n", decoded.sort->keys.size());
        }
        if (decoded.join.has_value()) {
            fprintf(stderr, "[cudf] join: %zu left keys, %zu right keys, right_table='%s'\n",
                    decoded.join->left_key_indices.size(),
                    decoded.join->right_key_indices.size(),
                    decoded.join_right_table_name.c_str());
        }
        if (!decoded.error_msg.empty())
            fprintf(stderr, "[cudf] decode error: %s\n", decoded.error_msg.c_str());
    }
    if (!decoded.ok) {
        write_error(error_msg, error_msg_len,
                    ("Substrait decode failed: " + decoded.error_msg).c_str());
        return -ENOSYS;
    }

    // 2. Find or auto-load the primary table handle
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
        // 3. Execute operation chain: Filter → Aggregate → Join → Sort → Distinct
        //
        // We use a "working table" pattern: each operation consumes the current
        // working table and produces a new one.
        std::unique_ptr<cudf::table> owned_table;   // Owns intermediate results
        cudf::table_view working_view = src_handle->view();
        std::vector<std::string> working_col_names = src_handle->column_names;

        // ── 3a. Filter ──────────────────────────────────────────────────
        if (decoded.filter) {
            owned_table = apply_filter(src_handle, *decoded.filter);
            working_view = owned_table->view();
            // Column names unchanged after filter
        }

        // ── 3b. Join ────────────────────────────────────────────────────
        if (decoded.join) {
            // Load or find the right table
            GpuTableHandle* right_handle = nullptr;
            std::unique_ptr<GpuTableHandle> right_auto;

            if (!decoded.join_right_file_path.empty()) {
                right_auto.reset(cudf_load_parquet(
                    decoded.join_right_file_path.c_str(), nullptr, 0));
                if (!right_auto) {
                    write_error(error_msg, error_msg_len,
                                ("cudf_load_parquet failed for right table: " +
                                 decoded.join_right_file_path).c_str());
                    return -EIO;
                }
                right_handle = right_auto.get();
            } else {
                for (int i = 0; i < num_tables; ++i) {
                    if (table_names[i] &&
                        decoded.join_right_table_name == table_names[i]) {
                        right_handle = table_handles[i];
                        break;
                    }
                }
                if (!right_handle) {
                    write_error(error_msg, error_msg_len,
                                ("Right table not found: " +
                                 decoded.join_right_table_name).c_str());
                    return -ENOENT;
                }
            }

            auto [join_tbl, join_names] = apply_hash_join(
                working_view, working_col_names,
                right_handle->view(), right_handle->column_names,
                *decoded.join);
            owned_table = std::move(join_tbl);
            working_view = owned_table->view();
            working_col_names = std::move(join_names);
        }

        // ── 3c. Aggregate ───────────────────────────────────────────────
        if (decoded.aggregate) {
            auto [agg_tbl, agg_names] = apply_hash_aggregate(
                working_view, working_col_names, *decoded.aggregate);
            owned_table = std::move(agg_tbl);
            working_view = owned_table->view();
            working_col_names = std::move(agg_names);
        }

        // ── 3d. Sort ────────────────────────────────────────────────────
        if (decoded.sort) {
            auto sorted_tbl = apply_sort(working_view, working_col_names,
                                          *decoded.sort);
            owned_table = std::move(sorted_tbl);
            working_view = owned_table->view();
            // Column names unchanged after sort
        }

        // ── 3e. Distinct ────────────────────────────────────────────────
        if (decoded.distinct) {
            auto distinct_tbl = apply_distinct(working_view, *decoded.distinct);
            owned_table = std::move(distinct_tbl);
            working_view = owned_table->view();
            // Column names unchanged after distinct
        }

        // 4. Build Arrow column metadata for schema export
        std::vector<cudf::column_metadata> col_meta;
        for (const auto& name : working_col_names) {
            col_meta.emplace_back(name);
        }

        // 5. Export schema
        auto schema_uptr = cudf::to_arrow_schema(working_view, col_meta);
        *out_schema = *schema_uptr;
        schema_uptr->release = nullptr;  // Transfer ownership to caller

        // 6. D2H transfer: GPU → CPU via Arrow C Data Interface
        auto arr_uptr = cudf::to_arrow_host(working_view);
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
