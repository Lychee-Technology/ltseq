//! Grouping operations for LTSeqTable
//!
//! Provides consecutive group identification and first/last row selection.
//!
//! # Operations
//!
//! ## group_id_impl
//!
//! Adds `__group_id__`, `__group_count__`, and `__rn__` columns to identify
//! consecutive groups. Supports both simple column references and complex
//! expressions containing window functions (e.g., `shift()`, `diff()`).
//!
//! Uses a two-phase approach:
//! 1. **Lazy phase**: Build boundary detection + cumulative sum → `__group_id__`
//!    via native DataFusion Expr API (`df.select()` — no materialization)
//! 2. **Window phase**: Add `__group_count__` and `__rn__` via native windows partitioned by
//!    `__group_id__` (requires one materialization since these partition by a
//!    window function result)
//!
//! ## first_row_impl / last_row_impl
//!
//! Filters table to only first/last row per group. Requires `__group_id__` column
//! from prior flatten() call. Uses ROW_NUMBER() OVER (PARTITION BY __group_id__).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::group_window::original_column_exprs;
use crate::ops::linear_scan::{can_linear_scan, linear_scan_group_id};
use crate::transpiler::contains_window_function;
use crate::transpiler::pyexpr_to_datafusion;
use crate::transpiler::window_native::{finalize_window_expr, pyexpr_to_window_expr};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::functions_window::expr_fn::lag;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Public API Functions
// ============================================================================

/// Fast path: count groups without building metadata arrays.
///
/// Used for `group_ordered(cond).first().count()` — returns just the
/// number of groups (sessions) without allocating per-row arrays.
///
/// Runs with the GIL released: `lib.rs::group_ordered_count` deserializes the
/// predicate under the GIL and wraps this call in `gil::detached`. Both paths
/// below (the rayon Parquet scan and the general linear scan) are pure Rust
/// and report `LtseqError`.
pub fn group_ordered_count_impl(
    table: &LTSeqTable,
    py_expr: &PyExpr,
) -> Result<usize, LtseqError> {
    // Only works for linear scan predicates (shift-based boundary detection)
    if !can_linear_scan(py_expr) {
        return Err(LtseqError::Validation(
            "group_ordered_count only supports shift-based boundary predicates".into(),
        ));
    }

    // Parquet fast path (bypasses DataFusion entirely): parallel per-RG
    // counting with seam stitching. It falls back to the general path below
    // when the predicate has no fused form. A source it cannot read is an
    // error, not a fallback (see parallel_streaming_group_count).
    if let Some(ref parquet_path) = table.source_parquet_path {
        if !table.sort_specs.is_empty() {
            match crate::ops::parallel_scan::parallel_streaming_group_count(
                table,
                py_expr,
                parquet_path,
            ) {
                Ok(count) => return Ok(count),
                Err(e) => {
                    let msg = e.to_string();
                    if !msg.contains("PARALLEL_FALLBACK") {
                        return Err(e);
                    }
                    // Fall through to the general path.
                }
            }
        }
    }

    // General path: do the full group_id computation and count
    let result = linear_scan_group_id(table, py_expr)?;
    let df = result.require_df()?;
    // Filter __rn__ == 1 and count
    let filtered = (**df)
        .clone()
        .filter(
            Expr::Column(datafusion::common::Column::new_unqualified("__rn__"))
                .eq(Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), None)),
        )
        .map_err(|e| LtseqError::Runtime(e.to_string()))?;
    RUNTIME
        .block_on(filtered.count())
        .map_err(|e| LtseqError::Runtime(e.to_string()))
}

/// Add __group_id__, __group_count__, and __rn__ columns to identify consecutive groups.
///
/// Supports both simple column references and complex expressions containing
/// window functions (e.g., `(r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)`).
///
/// Two-phase approach:
/// 1. **Lazy phase**: Compute boundary boolean via native DataFusion Expr API
///    (`df.select()` — one window function level, no nesting)
/// 2. **SQL phase**: From the materialized `__boundary__` column, compute mask,
///    cumulative sum → `__group_id__`, then `__group_count__` and `__rn__`
pub fn group_id_impl(table: &LTSeqTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }
    let (df, schema) = table.require_df_and_schema()?;

    // Deserialize grouping expression
    let py_expr = dict_to_py_expr(&grouping_expr)?;

    // NOTE: no linear-scan branch here. linear_scan_group_id returns a
    // METADATA-ONLY table (__group_id__/__group_count__/__rn__ without the
    // user columns) — sufficient for counting, so group_ordered_count_impl
    // uses it as its fallback, but flatten()/derive()/filter()/first()/last()
    // all need the original columns. Routing shift-based predicates through
    // it silently dropped every user column from those operations
    // (pre-existing hole surfaced by the native group-window migration,
    // issue #91 PR 4). The window path below handles shift natively.

    let has_window = contains_window_function(&py_expr);

    // Build ORDER BY from table's sort specs (direction-aware)
    let order_by: Vec<Sort> = crate::metadata::sort_specs_to_window_sorts(&table.sort_specs);

    // ── Step 1: Build the boundary boolean expression ──────────────────
    let boundary_expr = if has_window {
        // The expression itself IS the boundary condition (contains shift/diff/etc.)
        // e.g., (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
        // Convert using the window-aware transpiler → native DataFusion Expr with LAG/LEAD
        pyexpr_to_window_expr(py_expr, schema, &table.sort_specs)
            .map_err(LtseqError::Validation)?
    } else {
        // Simple expression (e.g., column reference) — build:
        // expr IS DISTINCT FROM LAG(expr, 1) OVER (ORDER BY sort_exprs)
        let expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(LtseqError::Validation)?;
        let lag_expr = lag(expr.clone(), Some(1), None);
        let lag_with_order = finalize_window_expr(lag_expr, vec![], &order_by, "group_id")
            .map_err(LtseqError::Validation)?;
        // IS DISTINCT FROM handles NULLs correctly (unlike !=)
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(expr),
            Operator::IsDistinctFrom,
            Box::new(lag_with_order),
        ))
    };

    // ── Step 2: Lazy select — all original columns + __boundary__ ─────
    // This is a flat window function (no nesting), so DataFusion can handle it
    let mut select_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(datafusion::common::Column::new_unqualified(f.name())))
        .collect();
    select_exprs.push(boundary_expr.alias("__boundary__"));

    let df_with_boundary = (**df)
        .clone()
        .select(select_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to add __boundary__ column: {}", e))
        })?;

    // ── Step 3: Native window expressions — no materialization ─────
    // From __boundary__ (boolean), build:
    //   mask = COALESCE(CASE WHEN __boundary__ THEN 1 ELSE 0 END, 1)
    //   __group_id__ = SUM(mask) OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)
    //   __group_count__ = COUNT(*) OVER (PARTITION BY __group_id__)
    //   __rn__ = ROW_NUMBER() OVER (PARTITION BY __group_id__)
    use datafusion::functions_aggregate::expr_fn::{count, sum};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::expr::{Case, WindowFunction, WindowFunctionParams};
    use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
    use datafusion::scalar::ScalarValue;

    // Build mask expression: CASE WHEN __boundary__ IS NULL THEN 1 WHEN __boundary__ THEN 1 ELSE 0 END
    let boundary_col = col("__boundary__");
    let mask_expr = Expr::Case(Case {
        expr: None,
        when_then_expr: vec![
            (
                Box::new(boundary_col.clone().is_null()),
                Box::new(lit(1i64)),
            ),
            (
                Box::new(boundary_col),
                Box::new(lit(1i64)),
            ),
        ],
        else_expr: Some(Box::new(lit(0i64))),
    });

    // Build __group_id__: SUM(mask) OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)
    let sum_mask = sum(mask_expr);
    let group_id_frame = WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::Null),
        WindowFrameBound::CurrentRow,
    );
    let group_id_expr = match sum_mask {
        Expr::AggregateFunction(agg) => Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(agg.func),
            params: WindowFunctionParams {
                args: agg.params.args,
                partition_by: vec![],
                // Accumulate in the DECLARED row order (issue #125): with an
                // empty ORDER BY the running sum follows physical order,
                // which is unstable on multi-partition plans. The Python
                // entry guard guarantees sort_specs is non-empty here.
                order_by: order_by.clone(),
                window_frame: group_id_frame,
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        })),
        _ => {
            return Err(LtseqError::Runtime(
                "Expected AggregateFunction from sum()".into(),
            )
            .into());
        }
    };

    // Build select expressions for step 3a: original columns + __group_id__ (drop __boundary__)
    let mut step3a_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(datafusion::common::Column::new_unqualified(f.name())))
        .collect();
    step3a_exprs.push(group_id_expr.alias("__group_id__"));

    let df_with_group_id = df_with_boundary
        .select(step3a_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to add __group_id__ column: {}", e))
        })?;

    // Build __group_count__: COUNT(*) OVER (PARTITION BY __group_id__)
    let group_id_col = col("__group_id__");
    let count_star = count(lit(1i64));
    let group_count_expr = match count_star {
        Expr::AggregateFunction(agg) => Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(agg.func),
            params: WindowFunctionParams {
                args: agg.params.args,
                partition_by: vec![group_id_col.clone()],
                order_by: vec![],
                window_frame: WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::Null),
                    WindowFrameBound::Following(ScalarValue::Null),
                ),
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        })),
        _ => {
            return Err(LtseqError::Runtime(
                "Expected AggregateFunction from count()".into(),
            )
            .into());
        }
    };

    // Build __rn__: ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY
    // <declared sort>) — in-group numbering must follow the declared row
    // order, not the physical partition order (issue #125).
    let rn_expr = row_number()
        .partition_by(vec![group_id_col])
        .order_by(order_by.clone())
        .build()
        .map_err(|e| LtseqError::Runtime(format!("Failed to build row_number window: {}", e)))?;

    // Build final select: original columns + __group_id__ + __group_count__ + __rn__
    let mut final_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(datafusion::common::Column::new_unqualified(f.name())))
        .collect();
    final_exprs.push(col("__group_id__"));
    final_exprs.push(group_count_expr.alias("__group_count__"));
    final_exprs.push(rn_expr.alias("__rn__"));

    let result_df = df_with_group_id
        .select(final_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to add __group_count__ and __rn__ columns: {}", e))
        })?;

    // Build result schema with internal columns
    let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        "__group_id__",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        "__group_count__",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        "__rn__",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let result_schema = Arc::new(ArrowSchema::new(result_fields));

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        result_schema,
        table.sort_specs.clone(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Get only the first row of each group
pub fn first_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    first_or_last_row_impl(table, true)
}

/// Get only the last row of each group
pub fn last_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    first_or_last_row_impl(table, false)
}

/// Shared implementation for first_row and last_row
fn first_or_last_row_impl(table: &LTSeqTable, is_first: bool) -> PyResult<LTSeqTable> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }
    let (df, schema) = table.require_df_and_schema()?;

    // Verify __group_id__ column exists
    if !schema.fields().iter().any(|f| f.name() == "__group_id__") {
        return Err(LtseqError::Validation(
            "__group_id__ column not found. Call flatten() first.".into(),
        )
        .into());
    }

    let has_rn = schema.fields().iter().any(|f| f.name() == "__rn__");
    let has_count = schema
        .fields()
        .iter()
        .any(|f| f.name() == "__group_count__");

    // Both group_id producers (the native window path and the linear scan)
    // emit __rn__ and __group_count__, so the error arm is unreachable for
    // flatten() products. Guard against direct misuse instead of keeping the
    // old collect → MemTable → SQL fallback alive (issue #91 PR 4).
    let (predicate, which) = match (is_first, has_rn, has_count) {
        (true, true, _) => (col("__rn__").eq(lit(1i64)), "first"),
        (false, true, true) => (col("__rn__").eq(col("__group_count__")), "last"),
        _ => {
            return Err(LtseqError::Validation(
                "first_row/last_row require __rn__ and __group_count__ from flatten(); \
                 legacy group_id tables without them are no longer supported"
                    .into(),
            )
            .into())
        }
    };

    let filtered = (**df).clone().filter(predicate).map_err(|e| {
        LtseqError::Runtime(format!("Failed to filter {} rows: {}", which, e))
    })?;

    // Drop the grouping columns by exact name (issue #214): a user column
    // such as `__rn_x` is not one of them.
    let keep_cols = original_column_exprs(filtered.schema().as_arrow());

    // If no user columns remain (metadata-only table from linear scan),
    // return the filtered DF as-is — count() still works on it.
    let result = if keep_cols.is_empty() {
        filtered
    } else {
        filtered.select(keep_cols).map_err(|e| {
            LtseqError::Runtime(format!("Failed to project {}_row result: {}", which, e))
        })?
    };

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result,
        Vec::new(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}
