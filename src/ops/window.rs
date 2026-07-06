//! Window function operations for derived columns
//!
//! This module handles window operations including:
//! - Shift operations (LAG/LEAD)
//! - Rolling aggregations (SUM, AVG, MIN, MAX, COUNT over windows)
//! - Cumulative sums with window frames
//! - Difference calculations
//!
//! Window functions are converted directly to DataFusion's native
//! `Expr::WindowFunction` via the `window_native` transpiler (issue #91):
//! the plan stays lazy and there is no SQL fallback. Two-level nested
//! windows (a window applied to another window's result, e.g.
//! `rolling(3).mean().shift(1)`) are planned as a staged pair of lazy
//! projections (issue #101) — plan-isomorphic to the manual two-step
//! derive. Deeper nesting and ranking-over-window shapes surface as errors
//! that suggest the two-step derive workaround.

use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_window_expr;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Column;
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Derive columns with window functions using native DataFusion expressions
///
/// This converts PyExpr window function calls directly to DataFusion `Expr::WindowFunction`
/// and uses `df.select()` to add derived columns — no materialization required.
pub fn derive_with_window_functions_impl(
    table: &LTSeqTable,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let schema = table
        .schema
        .as_ref()
        .ok_or(LtseqError::NoSchema)?;

    let df = table
        .dataframe
        .as_ref()
        .ok_or(LtseqError::NoData)?;

    // Deserialize all expressions once
    let parsed_cols = parse_derived_cols(derived_cols)?;

    derive_with_window_functions_from_parsed(table, schema, df, &parsed_cols)
}

/// Internal entry point when expressions are already parsed (avoids double deserialization).
pub(crate) fn derive_with_window_functions_from_parsed(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    parsed_cols: &[(String, PyExpr)],
) -> PyResult<LTSeqTable> {
    // NOTE: Arrow shift fast path disabled.
    // The native DataFusion window path (below) stays lazy and benefits from column
    // pruning and predicate pushdown, making it ~150x faster for Parquet sources
    // with many columns (e.g., 105-column ClickBench hits table: 0.28s native vs
    // 42s arrow shift due to eager collect of all columns).

    native_window_derive(table, schema, df, parsed_cols).map_err(|e| {
        nested_window_hint(e)
    })
}

/// Enrich native-transpile errors about window-on-window shapes with the
/// two-step derive workaround. Two-level nesting is planned natively by the
/// staged planner (issue #101); this hint now covers what remains — three
/// and deeper levels, and ranking-over-window shapes.
fn nested_window_hint(err: PyErr) -> PyErr {
    let msg = err.to_string();
    if msg.contains("requires DataFrame context") {
        LtseqError::Validation(format!(
            "{}. Window functions cannot be applied to another window function's \
             result in a single derive; split it into two steps, e.g. \
             t.derive(mid=lambda r: r.x.rolling(3).mean())\
             .derive(out=lambda r: r.mid.shift(1))",
            msg.trim_end_matches('.')
        ))
        .into()
    } else {
        err
    }
}

/// Deserialize derived column expressions from a Python dict into Vec<(String, PyExpr)>.
pub(crate) fn parse_derived_cols(derived_cols: &Bound<'_, PyDict>) -> PyResult<Vec<(String, PyExpr)>> {
    let mut parsed = Vec::with_capacity(derived_cols.len());
    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            LtseqError::Validation("Column name must be string".into())
        })?;
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            LtseqError::Validation("Expression must be dict".into())
        })?;
        let py_expr = dict_to_py_expr(expr_dict)?;
        parsed.push((col_name_str, py_expr));
    }
    Ok(parsed)
}


/// A derive request split into two lazy projections (issue #101).
///
/// `stage_cols` are hidden intermediate window columns (the INPUT of an
/// outer window that itself contains a window); `final_cols` are the user's
/// requested columns with those inputs rewritten to hidden-column
/// references. Empty `stage_cols` means no nesting — single-select path.
struct StagedWindowPlan {
    stage_cols: Vec<(String, PyExpr)>,
    final_cols: Vec<(String, PyExpr)>,
}

/// Recursive rewrite for `build_staged_plan`: hoists any window call's
/// window-bearing input via `hoist`, recursing through plain combinators.
/// Ranking (`PyExpr::Window`) nodes are deliberately not entered — nesting
/// inside them stays out of scope and falls to `nested_window_hint`.
fn rewrite_nested(
    expr: PyExpr,
    hoist: &mut dyn FnMut(PyExpr) -> PyExpr,
) -> PyExpr {
    match expr {
        PyExpr::Call { func, on, args, kwargs } => {
            if crate::transpiler::is_window_call(&func, &on) {
                // The window's input: for rolling aggregates the input
                // lives one level down (agg → rolling → input).
                if func != "rolling" && matches!(&*on, PyExpr::Call { func: f, .. } if f == "rolling") {
                    // agg over rolling: dig into the rolling call's input
                    if let PyExpr::Call { func: rfunc, on: rin, args: rargs, kwargs: rkwargs } = *on {
                        let new_rin = if crate::transpiler::contains_window_function(&rin) {
                            hoist(*rin)
                        } else {
                            rewrite_nested(*rin, hoist)
                        };
                        return PyExpr::Call {
                            func,
                            on: Box::new(PyExpr::Call {
                                func: rfunc,
                                on: Box::new(new_rin),
                                args: rargs,
                                kwargs: rkwargs,
                            }),
                            args,
                            kwargs,
                        };
                    }
                    unreachable!("matched rolling above");
                }
                // shift/diff/cum_sum/rolling: input is `on`
                let new_on = if crate::transpiler::contains_window_function(&on) {
                    hoist(*on)
                } else {
                    rewrite_nested(*on, hoist)
                };
                return PyExpr::Call { func, on: Box::new(new_on), args, kwargs };
            }
            // Plain call: recurse into on and args
            PyExpr::Call {
                func,
                on: Box::new(rewrite_nested(*on, hoist)),
                args: args.into_iter().map(|a| rewrite_nested(a, hoist)).collect(),
                kwargs,
            }
        }
        PyExpr::BinOp { op, left, right } => PyExpr::BinOp {
            op,
            left: Box::new(rewrite_nested(*left, hoist)),
            right: Box::new(rewrite_nested(*right, hoist)),
        },
        PyExpr::UnaryOp { op, operand } => PyExpr::UnaryOp {
            op,
            operand: Box::new(rewrite_nested(*operand, hoist)),
        },
        PyExpr::Alias { expr, alias } => PyExpr::Alias {
            expr: Box::new(rewrite_nested(*expr, hoist)),
            alias,
        },
        // Ranking windows: leave untouched — nesting inside them is out
        // of scope and falls through to nested_window_hint.
        other => other,
    }
}

/// Split two-level nested windows into a staged plan.
///
/// Whenever a window call's input subtree contains another window function
/// (`contains_window_function`), the WHOLE input expression is hoisted into
/// a hidden stage column (stage-1 can plan arbitrary window-bearing
/// expressions) and replaced by a column reference. `PyExpr::Window`
/// (ranking) nodes are deliberately left untouched: nesting inside them
/// still falls through to the nested_window_hint error.
fn build_staged_plan(
    schema: &ArrowSchema,
    parsed_cols: &[(String, PyExpr)],
) -> StagedWindowPlan {
    let mut used_names: std::collections::HashSet<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for (name, _) in parsed_cols {
        used_names.insert(name.clone());
    }

    let mut stage_cols: Vec<(String, PyExpr)> = Vec::new();
    let mut counter = 0usize;

    let mut hoist = |input: PyExpr| -> PyExpr {
        // Reuse an existing stage column for an identical input expression.
        for (name, expr) in stage_cols.iter() {
            if *expr == input {
                return PyExpr::Column(name.clone());
            }
        }
        let mut name = format!("__ltseq_stage_{}", counter);
        while used_names.contains(&name) {
            counter += 1;
            name = format!("__ltseq_stage_{}", counter);
        }
        counter += 1;
        used_names.insert(name.clone());
        stage_cols.push((name.clone(), input));
        PyExpr::Column(name)
    };

    let final_cols = parsed_cols
        .iter()
        .map(|(name, expr)| (name.clone(), rewrite_nested(expr.clone(), &mut hoist)))
        .collect();

    StagedWindowPlan { stage_cols, final_cols }
}

/// Native window derive: convert PyExpr window calls to DataFusion Expr and use df.select()
fn native_window_derive(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    parsed_cols: &[(String, PyExpr)],
) -> PyResult<LTSeqTable> {
    let plan = build_staged_plan(schema, parsed_cols);

    // Fast path: no nested windows — the existing single lazy projection.
    if plan.stage_cols.is_empty() {
        let mut all_exprs: Vec<Expr> = original_columns(schema);
        for (col_name_str, py_expr) in parsed_cols.iter() {
            let window_expr = pyexpr_to_window_expr(py_expr.clone(), schema, &table.sort_specs)
                .map_err(LtseqError::Transpile)?;
            all_exprs.push(window_expr.alias(col_name_str));
        }
        let result_df = (**df)
            .clone()
            .select(all_exprs)
            .map_err(|e| {
                LtseqError::Runtime(format!("Native window derive failed: {}", e))
            })?;
        return Ok(LTSeqTable::from_df(
            Arc::clone(&table.session),
            result_df,
            table.sort_specs.clone(),
            None, // column set changed relative to the raw file: drop fast-path token
        ));
    }

    // Staged path (issue #101): two lazy projections, plan-isomorphic to the
    // manual two-step derive.
    //
    // Stage 1: original columns + hidden inner-window columns.
    let mut stage1: Vec<Expr> = original_columns(schema);
    for (hidden_name, py_expr) in plan.stage_cols.iter() {
        let window_expr = pyexpr_to_window_expr(py_expr.clone(), schema, &table.sort_specs)
            .map_err(LtseqError::Transpile)?;
        stage1.push(window_expr.alias(hidden_name));
    }
    let stage_df = (**df)
        .clone()
        .select(stage1)
        .map_err(|e| LtseqError::Runtime(format!("Nested window stage failed: {}", e)))?;

    // Stage 2: original columns + the rewritten user expressions (hidden
    // columns are consumed here and NOT selected — they never reach the
    // result schema).
    let stage_schema = LTSeqTable::schema_from_df(stage_df.schema());
    let mut stage2: Vec<Expr> = original_columns(schema);
    for (col_name_str, py_expr) in plan.final_cols.iter() {
        let window_expr =
            pyexpr_to_window_expr(py_expr.clone(), &stage_schema, &table.sort_specs)
                .map_err(LtseqError::Transpile)?;
        stage2.push(window_expr.alias(col_name_str));
    }
    let result_df = stage_df
        .select(stage2)
        .map_err(|e| LtseqError::Runtime(format!("Native window derive failed: {}", e)))?;

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_specs.clone(),
        None, // column set changed relative to the raw file: drop fast-path token
    ))
}

/// All schema fields as unqualified column expressions.
fn original_columns(schema: &ArrowSchema) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect()
}

/// Add cumulative sum columns with proper window functions (fully native).
///
/// Empty inputs need no special casing: the window select over an empty
/// DataFrame yields an empty result whose schema already carries the new
/// cumsum columns.
pub fn cum_sum_impl(table: &LTSeqTable, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            table.sort_specs.clone(),
            None, // column set changed relative to the raw file: drop fast-path token
        ));
    }

    let (df, schema) = table.require_df_and_schema()?;
    native_cum_sum(table, schema, df, &cum_exprs)
}

/// Native cum_sum implementation using DataFusion Expr
fn native_cum_sum(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    cum_exprs: &[Bound<'_, PyDict>],
) -> PyResult<LTSeqTable> {
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use datafusion::scalar::ScalarValue;

    let mut all_exprs: Vec<Expr> = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        all_exprs.push(Expr::Column(Column::new_unqualified(field.name())));
    }

    // Build sort expressions from sort specs (direction-aware)
    let order_by: Vec<Sort> = crate::metadata::sort_specs_to_window_sorts(&table.sort_specs);

    // Add cumulative sum columns for each input column
    for (idx, expr_item) in cum_exprs.iter().enumerate() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            LtseqError::Validation("Expression must be dict".into())
        })?;

        let py_expr = dict_to_py_expr(expr_dict)?;

        // Extract column name
        let col_name = if let PyExpr::Column(name) = &py_expr {
            name.clone()
        } else {
            format!("cum_sum_{}", idx)
        };

        // Validate numeric column
        if let PyExpr::Column(col_name_ref) = &py_expr {
            if let Ok(field) = schema.field_with_name(col_name_ref) {
                let field_type = field.data_type();
                if !crate::transpiler::is_numeric_type(field_type) {
                    return Err(PyErr::from(LtseqError::TypeMismatch(format!(
                        "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                        col_name_ref, field_type
                    ))));
                }
            }
        }

        // Build native cum_sum expression: SUM(col) OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING TO CURRENT ROW)
        let mut col_expr = crate::transpiler::pyexpr_to_datafusion(py_expr.clone(), schema)
            .map_err(LtseqError::Transpile)?;

        // A Null-typed column (e.g. inferred from a header-only CSV) is
        // allowed by the numeric check but SUM(Null) has no return type.
        // Cast to Float64 — matching the Float64 cumsum column the legacy
        // empty-table path produced.
        if let PyExpr::Column(col_name_ref) = &py_expr {
            if let Ok(field) = schema.field_with_name(col_name_ref) {
                if field.data_type() == &datafusion::arrow::datatypes::DataType::Null {
                    col_expr = cast(col_expr, datafusion::arrow::datatypes::DataType::Float64);
                }
            }
        }

        // Reuse the window_native builder — the same aggregate→window
        // conversion convert_cum_sum/convert_rolling_agg use, so there is a
        // single cum_sum implementation (issue #91 PR 3 item 7).
        let sum_agg = datafusion::functions_aggregate::expr_fn::sum(col_expr);
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::Null), // UNBOUNDED PRECEDING
            WindowFrameBound::CurrentRow,
        );
        let cumsum_expr = crate::transpiler::window_native::aggregate_to_window(
            sum_agg,
            vec![],
            order_by.clone(),
            frame,
        )
        .map_err(LtseqError::Transpile)?;

        let cumsum_col_name = format!("{}_cumsum", col_name);
        all_exprs.push(cumsum_expr.alias(&cumsum_col_name));
    }

    // Apply via df.select() — stays lazy
    let result_df = (**df)
        .clone()
        .select(all_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Native cum_sum derive failed: {}", e))
        })?;

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_specs.clone(),
        None, // column set changed relative to the raw file: drop fast-path token
    ))
}
