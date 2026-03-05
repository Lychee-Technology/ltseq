#pragma once
/*
 * substrait_planner.hpp — Substrait plan decoder for the cuDF engine.
 *
 * Decodes a serialised substrait::Plan and produces a DecodedPlan that
 * the engine can execute directly using libcudf.
 *
 * Supported Substrait nodes:
 *   ReadRel       → Parquet table scan (uses pre-loaded GpuTableHandle)
 *   FilterRel     → cudf::apply_boolean_mask with a scalar predicate
 *   ProjectRel    → column selection / reordering
 *   AggregateRel  → cudf::groupby with hash aggregation
 *   SortRel       → cudf::sort / cudf::stable_sort
 *   JoinRel       → cudf::inner_join / left_join / full_join + gather
 *   HashJoinRel   → same as JoinRel (physical hint)
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

// ── Aggregate operation ──────────────────────────────────────────────────────

enum class AggFunc {
    SUM, MIN, MAX, COUNT, COUNT_ALL, MEAN, AVG
};

struct AggMeasure {
    AggFunc     func;
    int         value_col_idx;       // column index in the input (-1 for COUNT(*))
    std::string value_col_name;      // resolved name (empty for COUNT(*))
};

struct AggregateOp {
    std::vector<int>         group_col_indices;   // column indices for GROUP BY keys
    std::vector<std::string> group_col_names;     // resolved names for GROUP BY keys
    std::vector<AggMeasure>  measures;             // aggregation measures
};

// ── Sort operation ───────────────────────────────────────────────────────────

struct SortKey {
    int         col_idx;        // column index in the input
    std::string col_name;       // resolved name
    bool        ascending;      // true = ASC, false = DESC
    bool        nulls_first;    // true = NULLS FIRST, false = NULLS LAST
};

struct SortOp {
    std::vector<SortKey> keys;
};

// ── Join operation ───────────────────────────────────────────────────────────

enum class JoinType {
    INNER, LEFT, RIGHT, OUTER,
    LEFT_SEMI, LEFT_ANTI, RIGHT_SEMI, RIGHT_ANTI
};

struct JoinOp {
    JoinType                 type;
    std::vector<int>         left_key_indices;    // key column indices in left input
    std::vector<int>         right_key_indices;   // key column indices in right input
    std::vector<std::string> left_col_names;      // all column names from left input
    std::vector<std::string> right_col_names;     // all column names from right input
};

// ── Distinct operation ───────────────────────────────────────────────────────

struct DistinctOp {
    std::vector<int>         key_col_indices;  // column indices that determine uniqueness
    std::vector<std::string> key_col_names;    // resolved names
};

// ── Decoded plan ─────────────────────────────────────────────────────────────

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

    // Optional aggregate operation (GROUP BY + measures)
    std::optional<AggregateOp> aggregate;

    // Optional sort operation
    std::optional<SortOp> sort;

    // Optional join operation (requires a second input table)
    std::optional<JoinOp> join;

    // Second input table for joins
    std::string join_right_table_name;
    std::string join_right_file_path;

    // Optional distinct operation
    std::optional<DistinctOp> distinct;
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
