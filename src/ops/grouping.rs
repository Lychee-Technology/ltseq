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
//! 2. **SQL phase**: Add `__group_count__` and `__rn__` via SQL partitioned by
//!    `__group_id__` (requires one materialization since these partition by a
//!    window function result)
//!
//! ## first_row_impl / last_row_impl
//!
//! Filters table to only first/last row per group. Requires `__group_id__` column
//! from prior flatten() call. Uses ROW_NUMBER() OVER (PARTITION BY __group_id__).

use crate::engine::RUNTIME;
use crate::ops::linear_scan::{can_linear_scan, linear_scan_group_id};
use crate::transpiler::contains_window_function;
use crate::transpiler::pyexpr_to_datafusion;
use crate::transpiler::window_native::{finalize_window_expr, pyexpr_to_window_expr};
use crate::types::dict_to_py_expr;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::functions_window::expr_fn::lag;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

/// Get validated dataframe and schema, or return empty table
fn get_df_and_schema_or_empty(
    table: &LTSeqTable,
) -> Result<(&Arc<datafusion::dataframe::DataFrame>, &Arc<ArrowSchema>), LTSeqTable> {
    if table.dataframe.is_none() {
        return Err(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }
    let df = table.dataframe.as_ref().unwrap();
    let schema = table.schema.as_ref().ok_or_else(|| {
        LTSeqTable::empty(
            Arc::clone(&table.session),
            None,
            Vec::new(),
            table.source_parquet_path.clone(),
        )
    })?;
    Ok((df, schema))
}

/// Collect DataFrame to batches and get schema
fn collect_and_get_schema(
    df: &datafusion::dataframe::DataFrame,
    stored_schema: &Arc<ArrowSchema>,
) -> PyResult<(Vec<RecordBatch>, Arc<ArrowSchema>)> {
    let batches = RUNTIME
        .block_on(async {
            df.clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let batch_schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema).clone()));

    Ok((batches, batch_schema))
}

/// Register a temp table and return its name
fn register_temp_table(
    session: &SessionContext,
    schema: &Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<String> {
    let table_name = format!("__ltseq_{}_temp_{}", prefix, std::process::id());
    let _ = session.deregister_table(&table_name);

    let temp_table = MemTable::try_new(Arc::clone(schema), vec![batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create memory table: {}",
            e
        ))
    })?;

    session
        .register_table(&table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register temp table: {}",
                e
            ))
        })?;

    Ok(table_name)
}

/// Execute SQL query and collect results
fn execute_sql_query(
    session: &SessionContext,
    sql: &str,
    op_name: &str,
) -> PyResult<Vec<RecordBatch>> {
    RUNTIME
        .block_on(async {
            let result_df = session
                .sql(sql)
                .await
                .map_err(|e| format!("Failed to execute {} query: {}", op_name, e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect {} results: {}", op_name, e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

/// Create result LTSeqTable from batches
fn create_result_from_batches(
    session: &Arc<SessionContext>,
    result_batches: Vec<RecordBatch>,
    fallback_schema: Option<&Arc<ArrowSchema>>,
    _prefix: &str,
) -> PyResult<LTSeqTable> {
    match fallback_schema {
        Some(schema) => LTSeqTable::from_batches_with_schema(
            Arc::clone(session),
            result_batches,
            Arc::clone(schema),
            Vec::new(),
            None,
        ),
        None => LTSeqTable::from_batches(
            Arc::clone(session),
            result_batches,
            Vec::new(),
            None,
        ),
    }
}

/// Build column list string from schema, optionally filtering internal columns
fn build_column_list(schema: &ArrowSchema, filter_internal: bool) -> String {
    schema
        .fields()
        .iter()
        .filter(|f| {
            if filter_internal {
                let name = f.name();
                !name.starts_with("__rn")
                    && !name.starts_with("__row_num")
                    && !name.starts_with("__mask")
                    && !name.starts_with("__cnt")
            } else {
                true
            }
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Fast path: count groups without building metadata arrays.
///
/// Used for `group_ordered(cond).first().count()` — returns just the
/// number of groups (sessions) without allocating per-row arrays.
pub fn group_ordered_count_impl(
    table: &LTSeqTable,
    grouping_expr: Bound<'_, PyDict>,
) -> PyResult<usize> {
    // Deserialize grouping expression
    let py_expr = dict_to_py_expr(&grouping_expr)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // Only works for linear scan predicates (shift-based boundary detection)
    if !can_linear_scan(&py_expr) {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "group_ordered_count only supports shift-based boundary predicates",
        ));
    }

    // Try direct Parquet streaming (bypasses DataFusion)
    if let Some(ref parquet_path) = table.source_parquet_path {
        if !table.sort_exprs.is_empty() {
            match crate::ops::parallel_scan::direct_streaming_group_count(
                table,
                &py_expr,
                parquet_path,
            ) {
                Ok(count) => return Ok(count),
                Err(e) => {
                    let msg = e.to_string();
                    if !msg.contains("PARALLEL_FALLBACK") {
                        return Err(e);
                    }
                    // Fall through to full path
                }
            }
        }
    }

    // Fallback: do the full group_id computation and count
    let result = linear_scan_group_id(table, &py_expr)?;
    let count = RUNTIME.block_on(async {
        let df = result
            .dataframe
            .as_ref()
            .ok_or_else(|| "No data".to_string())?;
        // Filter __rn__ == 1 and count
        let filtered = (**df)
            .clone()
            .filter(
                Expr::Column(datafusion::common::Column::new_unqualified("__rn__"))
                    .eq(Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), None)),
            )
            .map_err(|e| e.to_string())?;
        filtered.count().await.map_err(|e| e.to_string())
    });

    match count {
        Ok(c) => Ok(c),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
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
    let (df, schema) = match get_df_and_schema_or_empty(table) {
        Ok(v) => v,
        Err(empty_table) => return Ok(empty_table),
    };

    // Deserialize grouping expression
    let py_expr = dict_to_py_expr(&grouping_expr)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // ── Fast path: linear scan engine ─────────────────────────────────────
    // For predicates containing shift(1) + comparisons + arithmetic + logic,
    // evaluate the boundary condition in a single O(n) pass instead of
    // multiple DataFusion window function passes.
    if can_linear_scan(&py_expr) {
        return linear_scan_group_id(table, &py_expr);
    }

    let has_window = contains_window_function(&py_expr);

    // Build ORDER BY from table's sort_exprs
    let order_by: Vec<Sort> = table
        .sort_exprs
        .iter()
        .map(|col_name| Sort {
            expr: Expr::Column(datafusion::common::Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect();

    // ── Step 1: Build the boundary boolean expression ──────────────────
    let boundary_expr = if has_window {
        // The expression itself IS the boundary condition (contains shift/diff/etc.)
        // e.g., (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
        // Convert using the window-aware transpiler → native DataFusion Expr with LAG/LEAD
        pyexpr_to_window_expr(py_expr, schema, &table.sort_exprs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?
    } else {
        // Simple expression (e.g., column reference) — build:
        // expr IS DISTINCT FROM LAG(expr, 1) OVER (ORDER BY sort_exprs)
        let expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        let lag_expr = lag(expr.clone(), Some(1), None);
        let lag_with_order = finalize_window_expr(lag_expr, vec![], &order_by, "group_id")
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
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
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to add __boundary__ column: {}",
                e
            ))
        })?;

    // ── Step 3: Materialize + compute group_id, count, rn via SQL ─────
    // From __boundary__ (boolean), build:
    //   mask = COALESCE(CASE WHEN __boundary__ THEN 1 ELSE 0 END, 1)
    //   __group_id__ = SUM(mask) OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)
    //   __group_count__ = COUNT(*) OVER (PARTITION BY __group_id__)
    //   __rn__ = ROW_NUMBER() OVER (PARTITION BY __group_id__)
    let batches = RUNTIME
        .block_on(async {
            df_with_boundary
                .collect()
                .await
                .map_err(|e| format!("Failed to collect boundary data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let batch_schema = batches[0].schema();
    let temp_table_name = register_temp_table(&table.session, &batch_schema, batches, "group_id")?;

    // Build column list excluding __boundary__ (internal column)
    let columns_str = batch_schema
        .fields()
        .iter()
        .filter(|f| f.name() != "__boundary__")
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let sql_query = format!(
        r#"WITH grouped AS (
          SELECT {cols},
            SUM(COALESCE(CASE WHEN "__boundary__" THEN 1 ELSE 0 END, 1))
              OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as __group_id__
          FROM "{table}"
        )
        SELECT {cols}, __group_id__,
          COUNT(*) OVER (PARTITION BY __group_id__) as __group_count__,
          ROW_NUMBER() OVER (PARTITION BY __group_id__) as __rn__
        FROM grouped"#,
        cols = columns_str,
        table = temp_table_name,
    );

    let result_batches = execute_sql_query(&table.session, &sql_query, "group_id")?;

    // Cleanup and return result
    let _ = table.session.deregister_table(&temp_table_name);
    create_result_from_batches(
        &table.session,
        result_batches,
        table.schema.as_ref(),
        "group_id",
    )
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
    let (df, schema) = match get_df_and_schema_or_empty(table) {
        Ok(v) => v,
        Err(empty_table) => return Ok(empty_table),
    };

    // Verify __group_id__ column exists
    if !schema.fields().iter().any(|f| f.name() == "__group_id__") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "__group_id__ column not found. Call flatten() first.",
        ));
    }

    let has_rn = schema.fields().iter().any(|f| f.name() == "__rn__");
    let has_count = schema
        .fields()
        .iter()
        .any(|f| f.name() == "__group_count__");

    // ── Fast path: use existing __rn__ / __group_count__ columns ─────
    // When linear_scan_group_id produced these columns lazily, we can
    // filter directly without SQL ROW_NUMBER overhead.
    if is_first && has_rn {
        // first row = __rn__ == 1
        let filtered = (**df)
            .clone()
            .filter(col("__rn__").eq(lit(1i64)))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to filter first rows: {}",
                    e
                ))
            })?;

        // Remove internal columns (__rn__, __group_id__, __group_count__)
        let keep_cols: Vec<Expr> = filtered
            .schema()
            .fields()
            .iter()
            .filter(|f| {
                let name = f.name();
                !name.starts_with("__rn")
                    && !name.starts_with("__group_id__")
                    && !name.starts_with("__group_count__")
                    && !name.starts_with("__row_num")
                    && !name.starts_with("__mask")
                    && !name.starts_with("__cnt")
            })
            .map(|f| col(f.name()))
            .collect();

        // If no non-internal columns remain (metadata-only table from linear scan),
        // return the filtered DF as-is — count() will still work correctly.
        if keep_cols.is_empty() {
            return Ok(LTSeqTable::from_df(
                Arc::clone(&table.session),
                filtered,
                Vec::new(),
                table.source_parquet_path.clone(),
            ));
        }

        let projected = filtered.select(keep_cols).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to project first_row result: {}",
                e
            ))
        })?;

        return Ok(LTSeqTable::from_df(
            Arc::clone(&table.session),
            projected,
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    if !is_first && has_rn && has_count {
        // last row = __rn__ == __group_count__
        let filtered = (**df)
            .clone()
            .filter(col("__rn__").eq(col("__group_count__")))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to filter last rows: {}",
                    e
                ))
            })?;

        // Remove internal columns
        let keep_cols: Vec<Expr> = filtered
            .schema()
            .fields()
            .iter()
            .filter(|f| {
                let name = f.name();
                !name.starts_with("__rn")
                    && !name.starts_with("__group_id__")
                    && !name.starts_with("__group_count__")
                    && !name.starts_with("__row_num")
                    && !name.starts_with("__mask")
                    && !name.starts_with("__cnt")
            })
            .map(|f| col(f.name()))
            .collect();

        if keep_cols.is_empty() {
            return Ok(LTSeqTable::from_df(
                Arc::clone(&table.session),
                filtered,
                Vec::new(),
                table.source_parquet_path.clone(),
            ));
        }

        let projected = filtered.select(keep_cols).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to project last_row result: {}",
                e
            ))
        })?;

        return Ok(LTSeqTable::from_df(
            Arc::clone(&table.session),
            projected,
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    // ── Fallback: SQL-based approach for legacy group_id paths ────────
    // Collect data and register temp table
    let (batches, batch_schema) = collect_and_get_schema(df, schema)?;
    let op_name = if is_first { "first_row" } else { "last_row" };
    let temp_table_name = register_temp_table(&table.session, &batch_schema, batches, op_name)?;
    let columns_str = build_column_list(&batch_schema, true);

    // Build SQL query
    let rn_alias = format!("__rn_{}_{}", op_name, std::process::id());
    let sql_query = if is_first {
        format!(
            r#"WITH ranked AS (
              SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}" FROM "{table}"
            )
            SELECT {cols} FROM ranked WHERE "{rn}" = 1"#,
            cols = columns_str,
            table = temp_table_name,
            rn = rn_alias
        )
    } else {
        let cnt_alias = format!("__cnt_{}_{}", op_name, std::process::id());
        format!(
            r#"WITH ranked AS (
              SELECT {cols}, 
                      ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}",
                      COUNT(*) OVER (PARTITION BY "__group_id__") as "{cnt}"
              FROM "{table}"
            )
            SELECT {cols} FROM ranked WHERE "{rn}" = "{cnt}""#,
            cols = columns_str,
            table = temp_table_name,
            rn = rn_alias,
            cnt = cnt_alias
        )
    };

    let result_batches = execute_sql_query(&table.session, &sql_query, op_name)?;

    // Cleanup and return result
    let _ = table.session.deregister_table(&temp_table_name);
    create_result_from_batches(
        &table.session,
        result_batches,
        table.schema.as_ref(),
        op_name,
    )
}
