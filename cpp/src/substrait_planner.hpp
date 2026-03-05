#pragma once
/*
 * substrait_planner.hpp — Substrait plan decoder for the cuDF engine.
 *
 * Decodes a serialised substrait::Plan and produces a DecodedPlan that
 * the engine can execute directly using libcudf.
 *
 * Supported Substrait nodes (for LTSeq Phase 4):
 *   ReadRel  → Parquet table scan (uses pre-loaded GpuTableHandle)
 *   FilterRel → cudf::apply_boolean_mask with a scalar predicate
 *   ProjectRel → column selection / reordering
 *
 * Unsupported nodes return a DecodedPlan with ok=false so the Rust side
 * can fall back to CPU execution gracefully.
 */

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

// ── Decoded operation types ───────────────────────────────────────────────────

enum class CompareOp { GT, GTE, LT, LTE, EQ, NEQ };

struct FilterPred {
    std::string column_name;
    CompareOp   op;
    // Scalar threshold — int64 or double
    std::variant<int64_t, double> threshold;
};

struct DecodedPlan {
    bool ok = false;           // false → fall back to CPU
    std::string error_msg;

    // Source table name (from ReadRel.named_table)
    std::string table_name;

    // Source file path (from ReadRel.local_files, stripped of "file://" prefix)
    // Non-empty means the C++ engine should auto-load this file via cudf_load_parquet()
    std::string local_file_path;

    // Optional filter predicate
    std::optional<FilterPred> filter;

    // Column projection (empty = all columns)
    std::vector<std::string> project_columns;
};

// ── Public API ────────────────────────────────────────────────────────────────

/// Decode a serialised Substrait Plan protobuf.
///
/// @param plan_bytes  Pointer to the raw protobuf bytes.
/// @param plan_len    Number of bytes.
/// @param table_names Array of table name strings that map handles → names.
/// @param num_tables  Number of entries in table_names.
/// @return A DecodedPlan with ok=true on success.
DecodedPlan decode_substrait(
    const uint8_t* plan_bytes,
    size_t         plan_len,
    const char**   table_names,
    int            num_tables);
