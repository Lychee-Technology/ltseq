/*
 * substrait_planner.cpp — Substrait decoder for the cuDF engine.
 *
 * Decodes the subset of Substrait plans that datafusion-substrait generates
 * for queries involving filter, aggregate, sort, join, and distinct.
 *
 * The Substrait comparison functions are identified by their URI:
 *   https://github.com/substrait-io/substrait/blob/main/extensions/
 *     functions_comparison.yaml
 *
 * Function names used by datafusion-substrait (v52.x):
 *   lt:any_any  lte:any_any  gt:any_any  gte:any_any  equal:any_any
 *   not_equal:any_any
 *
 * Aggregate function names:
 *   sum:opt_i32  sum:opt_i64  sum:opt_f32  sum:opt_f64
 *   min:opt_*  max:opt_*  count:opt_*  count:*  avg:opt_*
 */

#include "substrait_planner.hpp"

// Generated protobuf headers (in CMAKE_BINARY_DIR/substrait_gen/)
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

// ── Extension URI → compare op mapping ───────────────────────────────────────

static CompareOp parse_function_name(const std::string& name) {
    // datafusion-substrait uses names like "lt:any_any"
    auto colon = name.find(':');
    std::string base = (colon != std::string::npos) ? name.substr(0, colon) : name;
    if (base == "lt")        return CompareOp::LT;
    if (base == "lte")       return CompareOp::LTE;
    if (base == "gt")        return CompareOp::GT;
    if (base == "gte")       return CompareOp::GTE;
    if (base == "equal")     return CompareOp::EQ;
    if (base == "not_equal") return CompareOp::NEQ;
    throw std::runtime_error("Unsupported function: " + name);
}

// ── Aggregate function name parser ───────────────────────────────────────────

/// Parse a Substrait aggregate function name to our AggFunc enum.
/// DataFusion-Substrait generates names like "sum:opt_i32", "count:opt_i64",
/// "avg:opt_f64", "min:opt_i32", etc.
static AggFunc parse_agg_function_name(const std::string& name) {
    auto colon = name.find(':');
    std::string base = (colon != std::string::npos) ? name.substr(0, colon) : name;
    if (base == "sum")         return AggFunc::SUM;
    if (base == "min")         return AggFunc::MIN;
    if (base == "max")         return AggFunc::MAX;
    if (base == "count")       return AggFunc::COUNT;
    if (base == "avg")         return AggFunc::AVG;
    if (base == "mean")        return AggFunc::MEAN;
    throw std::runtime_error("Unsupported aggregate function: " + name);
}

// ── Internal decoder ─────────────────────────────────────────────────────────

struct PlanDecoder {
    // Map function_reference → function name (from extension declarations)
    std::unordered_map<uint32_t, std::string> func_map;

    // The table names provided by the Rust caller
    const char** table_names;
    int num_tables;

    explicit PlanDecoder(const char** tnames, int n)
        : table_names(tnames), num_tables(n) {}

    void load_extensions(const substrait::Plan& plan) {
        for (const auto& ext : plan.extensions()) {
            if (ext.has_extension_function()) {
                const auto& fn = ext.extension_function();
                func_map[fn.function_anchor()] = fn.name();
            }
        }
    }

    // Decode an Expression into a FilterPred component.
    // Returns false if the expression is unsupported.
    bool decode_expression(
        const substrait::Expression& expr,
        const std::vector<std::string>& col_names,
        FilterPred& out_pred)
    {
        if (!expr.has_scalar_function()) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: not a scalar_function (case=%d)\n",
                        (int)expr.rex_type_case());
            return false;  // Only simple scalar functions supported
        }

        const auto& sf = expr.scalar_function();
        auto it = func_map.find(sf.function_reference());
        if (it == func_map.end()) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: unknown func_ref=%u\n",
                        sf.function_reference());
            return false;
        }

        CompareOp op;
        try { op = parse_function_name(it->second); }
        catch (...) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: parse_function_name failed: %s\n",
                        it->second.c_str());
            return false;
        }

        // Expect exactly 2 arguments: FieldReference and Literal
        if (sf.arguments_size() != 2) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: expected 2 args, got %d\n",
                        sf.arguments_size());
            return false;
        }

        const auto& arg0 = sf.arguments(0).value();
        const auto& arg1 = sf.arguments(1).value();

        // Arg0 = FieldReference (column), Arg1 = Literal (threshold)
        if (!arg0.has_selection() || !arg1.has_literal()) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: arg0.has_selection=%d arg1.has_literal=%d arg0_case=%d arg1_case=%d\n",
                        arg0.has_selection(), arg1.has_literal(),
                        (int)arg0.rex_type_case(), (int)arg1.rex_type_case());
            return false;
        }

        const auto& sel = arg0.selection();
        if (!sel.has_direct_reference() ||
            !sel.direct_reference().has_struct_field()) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: no direct_reference/struct_field\n");
            return false;
        }

        int field_idx = sel.direct_reference().struct_field().field();
        if (field_idx < 0 || static_cast<size_t>(field_idx) >= col_names.size()) {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: field_idx=%d out of range (col_names.size=%zu)\n",
                        field_idx, col_names.size());
            return false;
        }

        const auto& lit = arg1.literal();
        std::variant<int64_t, double> threshold;
        if (lit.has_i64())    threshold = lit.i64();
        else if (lit.has_i32()) threshold = static_cast<int64_t>(lit.i32());
        else if (lit.has_fp64()) threshold = lit.fp64();
        else if (lit.has_fp32()) threshold = static_cast<double>(lit.fp32());
        else {
            if (std::getenv("LTSEQ_GPU_DEBUG"))
                fprintf(stderr, "[cudf] decode_expression: unsupported literal type\n");
            return false;  // Unsupported literal type
        }

        out_pred.column_name = col_names[field_idx];
        out_pred.op          = op;
        out_pred.threshold   = threshold;
        return true;
    }

    // Decode join key expressions from a JoinRel's expression field.
    // Handles single equality (equal(left_ref, right_ref)) and
    // multi-key AND(equal(...), equal(...), ...) patterns.
    bool decode_join_keys(
        const substrait::Expression& expr,
        const std::vector<std::string>& left_col_names,
        const std::vector<std::string>& right_col_names,
        JoinOp& join_op)
    {
        if (!expr.has_scalar_function()) return false;

        const auto& sf = expr.scalar_function();
        auto it = func_map.find(sf.function_reference());
        if (it == func_map.end()) return false;

        auto colon = it->second.find(':');
        std::string base = (colon != std::string::npos)
            ? it->second.substr(0, colon) : it->second;

        if (base == "and" || base == "and_") {
            // AND of multiple equalities — recurse on each argument
            for (int i = 0; i < sf.arguments_size(); ++i) {
                if (sf.arguments(i).has_value()) {
                    if (!decode_join_keys(sf.arguments(i).value(),
                                          left_col_names, right_col_names,
                                          join_op))
                        return false;
                }
            }
            return true;
        }

        if (base == "equal") {
            // Single equality: equal(left_field_ref, right_field_ref)
            if (sf.arguments_size() != 2) return false;

            const auto& arg0 = sf.arguments(0).value();
            const auto& arg1 = sf.arguments(1).value();

            if (!arg0.has_selection() || !arg1.has_selection()) return false;

            auto extract_idx = [](const substrait::Expression& e) -> int {
                if (e.has_selection() &&
                    e.selection().has_direct_reference() &&
                    e.selection().direct_reference().has_struct_field()) {
                    return e.selection().direct_reference().struct_field().field();
                }
                return -1;
            };

            int idx0 = extract_idx(arg0);
            int idx1 = extract_idx(arg1);
            if (idx0 < 0 || idx1 < 0) return false;

            // In Substrait JoinRel, field references are relative to
            // the concatenated schema [left_cols | right_cols].
            // Left indices are [0, left_count), right are [left_count, ...).
            int left_count = static_cast<int>(left_col_names.size());

            if (idx0 < left_count && idx1 >= left_count) {
                // Normal: arg0=left, arg1=right
                join_op.left_key_indices.push_back(idx0);
                join_op.right_key_indices.push_back(idx1 - left_count);
            } else if (idx1 < left_count && idx0 >= left_count) {
                // Swapped: arg0=right, arg1=left
                join_op.left_key_indices.push_back(idx1);
                join_op.right_key_indices.push_back(idx0 - left_count);
            } else {
                // Both from same side — unsupported
                return false;
            }
            return true;
        }

        return false;  // Not an equality or AND
    }

    // Walk the Rel tree and fill in the DecodedPlan.
    bool decode_rel(const substrait::Rel& rel, DecodedPlan& plan,
                    std::vector<std::string>& col_names)
    {
        if (rel.has_read()) {
            const auto& read = rel.read();

            // Get the table name
            if (read.has_named_table()) {
                const auto& nt = read.named_table();
                if (nt.names_size() > 0) {
                    plan.table_name = nt.names(0);
                }
            } else if (read.has_local_files()) {
                // Extract the first file URI, strip "file://" prefix
                const auto& lf = read.local_files();
                if (lf.items_size() > 0) {
                    const auto& item = lf.items(0);
                    std::string uri;
                    if (item.has_uri_file())       uri = item.uri_file();
                    else if (item.has_uri_path())  uri = item.uri_path();
                    if (uri.substr(0, 7) == "file://") uri = uri.substr(7);
                    plan.local_file_path = uri;
                    // Use the file stem as the table name for the Substrait executor
                    auto slash = uri.rfind('/');
                    auto dot   = uri.rfind('.');
                    if (slash != std::string::npos && dot != std::string::npos && dot > slash)
                        plan.table_name = uri.substr(slash + 1, dot - slash - 1);
                    else
                        plan.table_name = uri;
                }
            } else if (!plan.table_name.empty()) {
                // Already set by parent
            } else {
                // Try the first registered table name as a fallback
                if (num_tables > 0 && table_names[0]) {
                    plan.table_name = table_names[0];
                }
            }

            // Extract output column names from base_schema
            std::vector<std::string> base_col_names;
            if (read.has_base_schema()) {
                for (const auto& name : read.base_schema().names()) {
                    base_col_names.push_back(name);
                }
            }

            // If the ReadRel has a projection (MaskExpression), build the projected
            // column list from the MaskExpression struct_items field indices.
            // Note: inline filter field references use BASE schema indices, not projected.
            if (read.has_projection() && read.projection().has_select()) {
                const auto& ss = read.projection().select();
                for (int i = 0; i < ss.struct_items_size(); ++i) {
                    int base_idx = ss.struct_items(i).field();
                    if (base_idx >= 0 && static_cast<size_t>(base_idx) < base_col_names.size()) {
                        col_names.push_back(base_col_names[base_idx]);
                        plan.project_columns.push_back(base_col_names[base_idx]);
                    }
                }
            } else {
                // No projection: col_names = full base schema
                col_names = base_col_names;
            }

            // Handle inline filter expression in ReadRel (field 4).
            // DataFusion-Substrait v52 puts the filter predicate directly
            // inside the ReadRel rather than as a separate FilterRel node.
            // Field references in the filter are ALWAYS relative to the
            // base schema (not the projected schema), so we use base_col_names.
            if (read.has_filter()) {
                FilterPred pred;
                const auto& filter_col_names =
                    base_col_names.empty() ? col_names : base_col_names;
                if (decode_expression(read.filter(), filter_col_names, pred)) {
                    plan.filter = pred;
                } else {
                    plan.ok = false;
                    plan.error_msg = "Unsupported inline filter expression (not a simple comparison)";
                    return false;
                }
            }
            return true;
        }

        if (rel.has_filter()) {
            const auto& filter = rel.filter();

            // First decode the input (ReadRel)
            if (!filter.has_input()) return false;
            if (!decode_rel(filter.input(), plan, col_names)) return false;

            // Now decode the filter condition
            if (filter.has_condition()) {
                FilterPred pred;
                if (decode_expression(filter.condition(), col_names, pred)) {
                    plan.filter = pred;
                } else {
                    // Unsupported filter expression — fall back to CPU
                    plan.ok = false;
                    plan.error_msg = "Unsupported filter expression (not a simple comparison)";
                    return false;
                }
            }
            return true;
        }

        if (rel.has_project()) {
            const auto& proj = rel.project();
            if (!proj.has_input()) return false;
            if (!decode_rel(proj.input(), plan, col_names)) return false;

            // Project: record which columns are selected by field index
            for (const auto& expr : proj.expressions()) {
                if (expr.has_selection() &&
                    expr.selection().has_direct_reference() &&
                    expr.selection().direct_reference().has_struct_field())
                {
                    int idx = expr.selection().direct_reference().struct_field().field();
                    if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) {
                        plan.project_columns.push_back(col_names[idx]);
                    }
                }
            }
            return true;
        }

        // ── AggregateRel ─────────────────────────────────────────────────
        if (rel.has_aggregate()) {
            const auto& agg = rel.aggregate();
            if (!agg.has_input()) return false;
            if (!decode_rel(agg.input(), plan, col_names)) return false;

            AggregateOp agg_op;

            // Decode grouping keys.
            // DataFusion-Substrait v52+ uses the top-level grouping_expressions
            // list with expression_references in each Grouping. Older plans use
            // the deprecated grouping_expressions inside each Grouping message.
            if (agg.grouping_expressions_size() > 0 && agg.groupings_size() > 0) {
                // New style: top-level grouping_expressions + references
                const auto& grp = agg.groupings(0);
                for (int i = 0; i < grp.expression_references_size(); ++i) {
                    uint32_t ref = grp.expression_references(i);
                    if (ref < static_cast<uint32_t>(agg.grouping_expressions_size())) {
                        const auto& gexpr = agg.grouping_expressions(ref);
                        if (gexpr.has_selection() &&
                            gexpr.selection().has_direct_reference() &&
                            gexpr.selection().direct_reference().has_struct_field())
                        {
                            int idx = gexpr.selection().direct_reference().struct_field().field();
                            if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) {
                                agg_op.group_col_indices.push_back(idx);
                                agg_op.group_col_names.push_back(col_names[idx]);
                            }
                        }
                    }
                }
            } else if (agg.groupings_size() > 0) {
                // Deprecated style: grouping_expressions inside Grouping
                const auto& grp = agg.groupings(0);
                for (int i = 0; i < grp.grouping_expressions_size(); ++i) {
                    const auto& gexpr = grp.grouping_expressions(i);
                    if (gexpr.has_selection() &&
                        gexpr.selection().has_direct_reference() &&
                        gexpr.selection().direct_reference().has_struct_field())
                    {
                        int idx = gexpr.selection().direct_reference().struct_field().field();
                        if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) {
                            agg_op.group_col_indices.push_back(idx);
                            agg_op.group_col_names.push_back(col_names[idx]);
                        }
                    }
                }
            }

            // Decode measures
            for (int i = 0; i < agg.measures_size(); ++i) {
                const auto& measure = agg.measures(i);
                if (!measure.has_measure()) continue;

                const auto& af = measure.measure();
                auto it = func_map.find(af.function_reference());
                if (it == func_map.end()) {
                    if (std::getenv("LTSEQ_GPU_DEBUG"))
                        fprintf(stderr, "[cudf] aggregate: unknown func_ref=%u\n",
                                af.function_reference());
                    plan.ok = false;
                    plan.error_msg = "Unsupported aggregate function reference";
                    return false;
                }

                AggFunc func;
                try { func = parse_agg_function_name(it->second); }
                catch (...) {
                    plan.ok = false;
                    plan.error_msg = "Unsupported aggregate function: " + it->second;
                    return false;
                }

                AggMeasure m;
                m.func = func;
                m.value_col_idx = -1;  // default for COUNT(*)

                // Extract the value column from the first argument
                if (af.arguments_size() > 0) {
                    const auto& arg = af.arguments(0);
                    if (arg.has_value() && arg.value().has_selection()) {
                        const auto& sel = arg.value().selection();
                        if (sel.has_direct_reference() &&
                            sel.direct_reference().has_struct_field()) {
                            int idx = sel.direct_reference().struct_field().field();
                            m.value_col_idx = idx;
                            if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) {
                                m.value_col_name = col_names[idx];
                            }
                        }
                    }
                }

                // COUNT(*) with no arguments or COUNT_ALL
                if (func == AggFunc::COUNT && m.value_col_idx < 0) {
                    m.func = AggFunc::COUNT_ALL;
                }

                agg_op.measures.push_back(std::move(m));
            }

            plan.aggregate = std::move(agg_op);

            // After aggregation, the output column names change:
            // group keys first, then measure results
            std::vector<std::string> new_col_names;
            for (const auto& name : plan.aggregate->group_col_names) {
                new_col_names.push_back(name);
            }
            for (size_t i = 0; i < plan.aggregate->measures.size(); ++i) {
                // Generate synthetic names for measure output columns
                const auto& m = plan.aggregate->measures[i];
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
                std::string col_name = func_str + "(" +
                    (m.value_col_name.empty() ? "*" : m.value_col_name) + ")";
                new_col_names.push_back(col_name);
            }
            col_names = std::move(new_col_names);
            return true;
        }

        // ── SortRel ──────────────────────────────────────────────────────
        if (rel.has_sort()) {
            const auto& sort_rel = rel.sort();
            if (!sort_rel.has_input()) return false;
            if (!decode_rel(sort_rel.input(), plan, col_names)) return false;

            SortOp sort_op;
            for (int i = 0; i < sort_rel.sorts_size(); ++i) {
                const auto& sf = sort_rel.sorts(i);
                SortKey key;
                key.col_idx = -1;

                // Extract column index from the sort expression
                if (sf.has_expr() && sf.expr().has_selection()) {
                    const auto& sel = sf.expr().selection();
                    if (sel.has_direct_reference() &&
                        sel.direct_reference().has_struct_field()) {
                        key.col_idx = sel.direct_reference().struct_field().field();
                        if (key.col_idx >= 0 &&
                            static_cast<size_t>(key.col_idx) < col_names.size()) {
                            key.col_name = col_names[key.col_idx];
                        }
                    }
                }

                if (key.col_idx < 0) {
                    plan.ok = false;
                    plan.error_msg = "Unsupported sort expression (not a column reference)";
                    return false;
                }

                // Decode sort direction
                if (sf.has_direction()) {
                    switch (sf.direction()) {
                        case substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST:
                            key.ascending = true;
                            key.nulls_first = true;
                            break;
                        case substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST:
                            key.ascending = true;
                            key.nulls_first = false;
                            break;
                        case substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST:
                            key.ascending = false;
                            key.nulls_first = true;
                            break;
                        case substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST:
                            key.ascending = false;
                            key.nulls_first = false;
                            break;
                        default:
                            key.ascending = true;
                            key.nulls_first = true;
                            break;
                    }
                } else {
                    key.ascending = true;
                    key.nulls_first = true;
                }

                sort_op.keys.push_back(std::move(key));
            }

            plan.sort = std::move(sort_op);
            return true;
        }

        // ── JoinRel ──────────────────────────────────────────────────────
        if (rel.has_join()) {
            const auto& join_rel = rel.join();
            if (!join_rel.has_left() || !join_rel.has_right()) return false;

            // Decode left input
            std::vector<std::string> left_col_names;
            if (!decode_rel(join_rel.left(), plan, left_col_names)) return false;

            // Decode right input — we need a separate plan for table info
            DecodedPlan right_plan;
            std::vector<std::string> right_col_names;
            if (!decode_rel(join_rel.right(), right_plan, right_col_names)) {
                plan.ok = false;
                plan.error_msg = "Failed to decode right side of JoinRel";
                return false;
            }
            plan.join_right_table_name = right_plan.table_name;
            plan.join_right_file_path = right_plan.local_file_path;

            JoinOp join_op;
            join_op.left_col_names = left_col_names;
            join_op.right_col_names = right_col_names;

            // Decode join type
            switch (join_rel.type()) {
                case substrait::JoinRel::JOIN_TYPE_INNER:
                    join_op.type = JoinType::INNER; break;
                case substrait::JoinRel::JOIN_TYPE_OUTER:
                    join_op.type = JoinType::OUTER; break;
                case substrait::JoinRel::JOIN_TYPE_LEFT:
                    join_op.type = JoinType::LEFT; break;
                case substrait::JoinRel::JOIN_TYPE_RIGHT:
                    join_op.type = JoinType::RIGHT; break;
                case substrait::JoinRel::JOIN_TYPE_LEFT_SEMI:
                    join_op.type = JoinType::LEFT_SEMI; break;
                case substrait::JoinRel::JOIN_TYPE_LEFT_ANTI:
                    join_op.type = JoinType::LEFT_ANTI; break;
                case substrait::JoinRel::JOIN_TYPE_RIGHT_SEMI:
                    join_op.type = JoinType::RIGHT_SEMI; break;
                case substrait::JoinRel::JOIN_TYPE_RIGHT_ANTI:
                    join_op.type = JoinType::RIGHT_ANTI; break;
                default:
                    plan.ok = false;
                    plan.error_msg = "Unsupported join type";
                    return false;
            }

            // Extract join keys from the join expression.
            // DataFusion-Substrait generates an equality expression like:
            //   equal(left_field_ref, right_field_ref)
            // For multi-key joins, it's AND(equal(...), equal(...), ...)
            if (join_rel.has_expression()) {
                if (!decode_join_keys(join_rel.expression(),
                                      left_col_names, right_col_names,
                                      join_op)) {
                    plan.ok = false;
                    plan.error_msg = "Unsupported join expression (not equality key refs)";
                    return false;
                }
            }

            plan.join = std::move(join_op);

            // Output columns: left columns then right columns
            col_names = left_col_names;
            col_names.insert(col_names.end(),
                             right_col_names.begin(), right_col_names.end());
            return true;
        }

        // ── HashJoinRel (physical hint, same semantics as JoinRel) ────────
        if (rel.has_hash_join()) {
            const auto& hj = rel.hash_join();
            if (!hj.has_left() || !hj.has_right()) return false;

            // Decode left input
            std::vector<std::string> left_col_names;
            if (!decode_rel(hj.left(), plan, left_col_names)) return false;

            // Decode right input
            DecodedPlan right_plan;
            std::vector<std::string> right_col_names;
            if (!decode_rel(hj.right(), right_plan, right_col_names)) {
                plan.ok = false;
                plan.error_msg = "Failed to decode right side of HashJoinRel";
                return false;
            }
            plan.join_right_table_name = right_plan.table_name;
            plan.join_right_file_path = right_plan.local_file_path;

            JoinOp join_op;
            join_op.left_col_names = left_col_names;
            join_op.right_col_names = right_col_names;

            // Decode join type
            switch (hj.type()) {
                case substrait::HashJoinRel::JOIN_TYPE_INNER:
                    join_op.type = JoinType::INNER; break;
                case substrait::HashJoinRel::JOIN_TYPE_OUTER:
                    join_op.type = JoinType::OUTER; break;
                case substrait::HashJoinRel::JOIN_TYPE_LEFT:
                    join_op.type = JoinType::LEFT; break;
                case substrait::HashJoinRel::JOIN_TYPE_RIGHT:
                    join_op.type = JoinType::RIGHT; break;
                case substrait::HashJoinRel::JOIN_TYPE_LEFT_SEMI:
                    join_op.type = JoinType::LEFT_SEMI; break;
                case substrait::HashJoinRel::JOIN_TYPE_LEFT_ANTI:
                    join_op.type = JoinType::LEFT_ANTI; break;
                case substrait::HashJoinRel::JOIN_TYPE_RIGHT_SEMI:
                    join_op.type = JoinType::RIGHT_SEMI; break;
                case substrait::HashJoinRel::JOIN_TYPE_RIGHT_ANTI:
                    join_op.type = JoinType::RIGHT_ANTI; break;
                default:
                    plan.ok = false;
                    plan.error_msg = "Unsupported hash join type";
                    return false;
            }

            // Extract keys from deprecated left_keys/right_keys fields
            for (int i = 0; i < hj.left_keys_size(); ++i) {
                const auto& lk = hj.left_keys(i);
                if (lk.has_direct_reference() &&
                    lk.direct_reference().has_struct_field()) {
                    join_op.left_key_indices.push_back(
                        lk.direct_reference().struct_field().field());
                }
            }
            for (int i = 0; i < hj.right_keys_size(); ++i) {
                const auto& rk = hj.right_keys(i);
                if (rk.has_direct_reference() &&
                    rk.direct_reference().has_struct_field()) {
                    join_op.right_key_indices.push_back(
                        rk.direct_reference().struct_field().field());
                }
            }

            // Also check the new-style `keys` field
            for (int i = 0; i < hj.keys_size(); ++i) {
                const auto& key = hj.keys(i);
                if (key.has_left() && key.left().has_direct_reference() &&
                    key.left().direct_reference().has_struct_field()) {
                    join_op.left_key_indices.push_back(
                        key.left().direct_reference().struct_field().field());
                }
                if (key.has_right() && key.right().has_direct_reference() &&
                    key.right().direct_reference().has_struct_field()) {
                    join_op.right_key_indices.push_back(
                        key.right().direct_reference().struct_field().field());
                }
            }

            plan.join = std::move(join_op);

            // Output columns: left columns then right columns
            col_names = left_col_names;
            col_names.insert(col_names.end(),
                             right_col_names.begin(), right_col_names.end());
            return true;
        }

        // Unsupported Rel type
        plan.error_msg = "Unsupported Rel type in Substrait plan";
        return false;
    }
};

// ── Public API ────────────────────────────────────────────────────────────────

DecodedPlan decode_substrait(
    const uint8_t* plan_bytes,
    size_t         plan_len,
    const char**   table_names,
    int            num_tables)
{
    DecodedPlan result;

    substrait::Plan plan;
    if (!plan.ParseFromArray(plan_bytes, static_cast<int>(plan_len))) {
        result.error_msg = "Failed to parse Substrait protobuf";
        return result;
    }

    if (plan.relations_size() == 0) {
        result.error_msg = "Substrait plan has no relations";
        return result;
    }

    PlanDecoder decoder(table_names, num_tables);
    decoder.load_extensions(plan);

    const auto& plan_rel = plan.relations(0);
    const substrait::Rel* rel = nullptr;

    if (plan_rel.has_root() && plan_rel.root().has_input()) {
        rel = &plan_rel.root().input();
    } else if (plan_rel.has_rel()) {
        rel = &plan_rel.rel();
    }

    if (!rel) {
        result.error_msg = "No Rel found in PlanRelation";
        return result;
    }

    std::vector<std::string> col_names;
    if (!decoder.decode_rel(*rel, result, col_names)) {
        return result;  // error_msg already set
    }

    result.ok = true;
    return result;
}
