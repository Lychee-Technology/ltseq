/*
 * substrait_planner.cpp — Substrait decoder for the cuDF engine.
 *
 * Decodes the subset of Substrait plans that datafusion-substrait generates
 * for simple filter queries (ReadRel + FilterRel + optional ProjectRel).
 *
 * The Substrait comparison functions are identified by their URI:
 *   https://github.com/substrait-io/substrait/blob/main/extensions/
 *     functions_comparison.yaml
 *
 * Function names used by datafusion-substrait (v52.x):
 *   lt:any_any  lte:any_any  gt:any_any  gte:any_any  equal:any_any
 *   not_equal:any_any
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
