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
//! Those are the column names when the input has no columns of them;
//! otherwise each takes the first free name of its kind (`__rn_1__`, ...),
//! and `group_id_impl` returns the names it chose as [`GroupColumns`], which
//! the caller hands to the operations that read them (#346).
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
//! Filters table to only first/last row per group, in group order. Requires
//! `__group_id__` column from prior flatten() call. Uses ROW_NUMBER() OVER
//! (PARTITION BY __group_id__).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::common::{column, free_names, select_windowed};
use crate::ops::group_window::{original_column_exprs, original_order};
use crate::ops::linear_scan::{can_linear_scan, linear_scan_group_id};
use crate::transpiler::contains_window_function;
use crate::transpiler::pyexpr_to_datafusion;
use crate::transpiler::window_native::{finalize_window_expr, pyexpr_to_window_expr};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
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

/// The names of the grouping columns of a `group_id_impl` product: the group
/// id, the number of rows of the group and the row's number within it.
///
/// Contract §6.4 reserves no column name, so these are chosen against the
/// grouped table's columns (`common::free_names`) and travel from the
/// producer to each operation that reads them, rather than being fixed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupColumns {
    pub id: String,
    pub count: String,
    pub rn: String,
}

impl Default for GroupColumns {
    /// The names a table without columns of these names gets, and the ones
    /// the metadata-only linear-scan table has.
    fn default() -> Self {
        GroupColumns {
            id: "__group_id__".into(),
            count: "__group_count__".into(),
            rn: "__rn__".into(),
        }
    }
}

impl From<(String, String, String)> for GroupColumns {
    fn from((id, count, rn): (String, String, String)) -> Self {
        GroupColumns { id, count, rn }
    }
}

impl From<GroupColumns> for (String, String, String) {
    fn from(names: GroupColumns) -> Self {
        (names.id, names.count, names.rn)
    }
}

impl GroupColumns {
    pub(crate) fn contains(&self, name: &str) -> bool {
        name == self.id || name == self.count || name == self.rn
    }
}

/// Fast path: count groups without building metadata arrays.
///
/// Used for `group_ordered(cond).first().count()` — returns just the
/// number of groups (sessions) without allocating per-row arrays.
///
/// Returns `Ok(None)` when this path does not count the input: the predicate
/// is not a shift-based boundary predicate the kernel computes as DataFusion
/// would, or the table's Parquet source is not one regular file (a directory
/// or a glob), which the direct reader cannot open. Python then counts the
/// materialized first rows. Every error is a genuine failure, which Python
/// reports and never retries (#152): a source the fast path cannot read is
/// one, so a read failure never turns into a fallback.
///
/// Runs with the GIL released: `lib.rs::group_ordered_count` deserializes the
/// predicate under the GIL and wraps this call in `gil::detached`. Both paths
/// below (the rayon Parquet scan and the general linear scan) are pure Rust
/// and report `LtseqError`.
pub fn group_ordered_count_impl(
    table: &LTSeqTable,
    py_expr: &PyExpr,
) -> Result<Option<usize>, LtseqError> {
    // Only works for linear scan predicates (shift-based boundary detection)
    // whose result the kernel computes as DataFusion would.
    if !can_linear_scan(py_expr, table.require_schema()?) {
        return Ok(None);
    }

    // Parquet fast path (bypasses DataFusion entirely): parallel per-RG
    // counting with seam stitching. It returns no count when the predicate
    // has no fused form, and the general path below counts instead. A source
    // it cannot read is an error, not a fallback (see
    // parallel_streaming_group_count).
    if let Some(ref parquet_path) = table.source_parquet_path {
        if !table.sort_specs.is_empty() {
            // The direct reader opens one file. A directory or a glob is
            // counted by Python through DataFusion, as it always was; sending
            // such sources to the general path below is #287. A path that
            // names nothing now (a glob, or a file deleted since the read) is
            // left to Python too, whose execution reports a deleted file as
            // every terminal does. Any other failure to look the path up is
            // an error, not a fallback (R3-F1 of #422's review).
            if !names_one_file(parquet_path, |p| std::fs::metadata(p))? {
                return Ok(None);
            }
            if let Some(count) = crate::ops::parallel_scan::parallel_streaming_group_count(
                table,
                py_expr,
                parquet_path,
            )? {
                return Ok(Some(count));
            }
        }
    }

    // General path: do the full group_id computation and count
    let result = linear_scan_group_id(table, py_expr)?;
    let df = result.require_df()?;
    // Filter __rn__ == 1 and count. The table holds only the grouping
    // columns, under their default names.
    let filtered = (**df)
        .clone()
        .filter(
            column(&GroupColumns::default().rn)
                .eq(Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), None)),
        )
        .map_err(|e| LtseqError::with_context("Failed to plan the group count", e))?;
    // A failed execution is the `LtseqError::collect` every terminal raises,
    // so this count and the materialized first rows raise one class.
    RUNTIME
        .block_on(filtered.count())
        .map(Some)
        .map_err(LtseqError::collect)
}

/// Whether `path` names a regular file now, following a link, looked up with
/// `metadata` (`std::fs::metadata`, or a stand-in in tests).
///
/// `Ok(false)` for a path the direct reader does not open: a directory, or a
/// path that names nothing (a glob, or a file deleted since the read). Any
/// other failure of the lookup, such as a permission or I/O error, is an
/// `LtseqError::Io` naming the path, so it is raised rather than retried.
fn names_one_file(
    path: &str,
    metadata: impl FnOnce(&str) -> std::io::Result<std::fs::Metadata>,
) -> Result<bool, LtseqError> {
    match metadata(path) {
        Ok(meta) => Ok(meta.is_file()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(LtseqError::io(
            &format!("Failed to look up Parquet source '{path}'"),
            e,
        )),
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
///
/// The four working columns take names no column of the table has and none
/// of `avoid` (#346); the result comes with the names of the three it keeps.
pub fn group_id_impl(
    table: &LTSeqTable,
    grouping_expr: Bound<'_, PyDict>,
    avoid: &[String],
) -> PyResult<(LTSeqTable, GroupColumns)> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok((
            LTSeqTable::unloaded(Arc::clone(&table.session)),
            GroupColumns::default(),
        ));
    }
    let (df, schema) = table.require_df_and_schema()?;

    // Deserialize grouping expression
    let py_expr = dict_to_py_expr(&grouping_expr)?;
    let [boundary, id, count, rn] = free_names(
        ["__boundary__", "__group_id__", "__group_count__", "__rn__"],
        schema,
        avoid,
    );
    let names = GroupColumns { id, count, rn };
    let grouped = crate::stack::planned(|| {
        group_id_parsed(table, df, schema, py_expr, &boundary, &names)
    })?;
    Ok((grouped, names))
}

/// [`group_id_impl`] once its expression is deserialized and its working
/// columns named.
fn group_id_parsed(
    table: &LTSeqTable,
    df: &Arc<DataFrame>,
    schema: &ArrowSchema,
    py_expr: PyExpr,
    boundary: &str,
    names: &GroupColumns,
) -> Result<LTSeqTable, LtseqError> {
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

    // The pipeline below runs over plain strings and binaries (#338); the
    // result has the input's types. The grouping expression is resolved
    // against the input's own types and reads each view column cast back to
    // its view type, so its types and coercions, and with them the groups,
    // are those of the input.
    let input_schema = schema;
    let (plain_df, plain_schema) = without_views(df, schema)?;
    let df = &plain_df;
    let schema = plain_schema.as_ref().unwrap_or(schema);
    let read_as_input = |expr: Expr| -> Result<Expr, LtseqError> {
        if plain_schema.is_some() {
            reading_views(expr, input_schema)
        } else {
            Ok(expr)
        }
    };

    // ── Step 1: Build the boundary boolean expression ──────────────────
    let boundary_expr = if has_window {
        // The expression itself IS the boundary condition (contains shift/diff/etc.)
        // e.g., (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
        // Convert using the window-aware transpiler → native DataFusion Expr with LAG/LEAD
        let expr = pyexpr_to_window_expr(py_expr, input_schema, &table.sort_specs)?;
        read_as_input(expr)?
    } else {
        // Simple expression (e.g., column reference) — build:
        // expr IS DISTINCT FROM LAG(expr, 1) OVER (ORDER BY sort_exprs)
        let expr = pyexpr_to_datafusion(py_expr, input_schema)?;
        let expr = read_as_input(expr)?;
        let lag_expr = lag(expr.clone(), Some(1), None);
        let lag_with_order = finalize_window_expr(lag_expr, vec![], &order_by, "group_id")?;
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
    select_exprs.push(boundary_expr.alias(boundary));

    // Each window function is staged under a name no input column has
    // (`select_windowed`, #346).
    let df_with_boundary = select_windowed((**df).clone(), select_exprs)
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
    let boundary_col = column(boundary);
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
            ));
        }
    };

    // Build select expressions for step 3a: original columns + __group_id__ (drop __boundary__)
    let mut step3a_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(datafusion::common::Column::new_unqualified(f.name())))
        .collect();
    step3a_exprs.push(group_id_expr.alias(&names.id));

    let df_with_group_id = select_windowed(df_with_boundary, step3a_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to add __group_id__ column: {}", e))
        })?;

    // Build __group_count__: COUNT(*) OVER (PARTITION BY __group_id__)
    let group_id_col = column(&names.id);
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
            ));
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
    final_exprs.push(column(&names.id));
    final_exprs.push(group_count_expr.alias(&names.count));
    // ROW_NUMBER is UInt64; the column is Int64, as the schema says.
    final_exprs.push(
        cast(rn_expr, datafusion::arrow::datatypes::DataType::Int64).alias(&names.rn),
    );

    // The windows partition by group, so their output can come back in any
    // order of groups; the sort by group and row number restores the
    // declared order (`with_plan_in_order` below), as the group window
    // operations end with.
    let windowed = select_windowed(df_with_group_id, final_exprs).map_err(|e| {
        LtseqError::Runtime(format!("Failed to add __group_count__ and __rn__ columns: {}", e))
    })?;
    // The cast back stays beneath the sort, which must be the plan's root
    // for `with_plan_in_order`.
    let windowed = if plain_schema.is_some() {
        with_views(windowed, input_schema)?
    } else {
        windowed
    };
    let result_df = windowed
        .sort(original_order(names))
        .map_err(|e| LtseqError::with_context("Failed to order the groups", e))?;

    // Build result schema with internal columns
    let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = input_schema
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        &names.id,
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        &names.count,
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    result_fields.push(datafusion::arrow::datatypes::Field::new(
        &names.rn,
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
    )
    .with_plan_in_order())
}

/// The plain type a view type of `data_type` stands for: the large one,
/// whose offsets do not overflow where a window or sort concatenates a whole
/// partition.
fn plain_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Utf8View => Some(DataType::LargeUtf8),
        DataType::BinaryView => Some(DataType::LargeBinary),
        _ => None,
    }
}

/// `df` with each `string_view` and `binary_view` column cast to its plain
/// type, and the schema it then has; `df` itself, and no schema, when it has
/// no such column (#338).
///
/// Arrow's `concat` of view arrays keeps every data buffer of every input,
/// and a slice or `take` of the result keeps them all again. Each sort and
/// window of the group pipeline concatenates what it holds and emits slices
/// of it, so in small batches the buffers a batch references multiply from
/// one operator to the next: a Parquet string column at batch size 1 took
/// seconds for 40 rows. Plain arrays copy their values when concatenated, so
/// a batch holds only its own.
fn without_views(
    df: &Arc<DataFrame>,
    schema: &ArrowSchema,
) -> Result<(Arc<DataFrame>, Option<ArrowSchema>), LtseqError> {
    if !schema.fields().iter().any(|f| plain_type(f.data_type()).is_some()) {
        return Ok((Arc::clone(df), None));
    }
    let exprs = schema
        .fields()
        .iter()
        .map(|f| {
            let column = Expr::Column(datafusion::common::Column::new_unqualified(f.name()));
            match plain_type(f.data_type()) {
                Some(plain) => cast(column, plain).alias(f.name()),
                None => column,
            }
        })
        .collect::<Vec<_>>();
    let plain = (**df)
        .clone()
        .select(exprs)
        .map_err(|e| LtseqError::with_context("Failed to cast view columns", e))?;
    let plain_schema = plain.schema().as_arrow().clone();
    Ok((Arc::new(plain), Some(plain_schema)))
}

/// `expr`, resolved against `schema`, reading each view column of `schema`
/// as a cast back from the plain copy `without_views` made of it (#338).
///
/// The grouping expression must keep the types it has over the input: the
/// length of a `string_view` is Int32 where that of a `large_string` is
/// Int64, so an expression resolved over the plain copies could compute
/// other keys, and other groups, than `derive` computes from the same
/// lambda. The casts are evaluated inside each operator, so the views they
/// make do not travel from one operator to the next.
fn reading_views(expr: Expr, schema: &ArrowSchema) -> Result<Expr, LtseqError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    expr.transform_up(|e| match &e {
        Expr::Column(c) => match schema.field_with_name(&c.name) {
            Ok(f) if plain_type(f.data_type()).is_some() => {
                Ok(Transformed::yes(cast(e.clone(), f.data_type().clone())))
            }
            _ => Ok(Transformed::no(e)),
        },
        _ => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
    .map_err(|e| LtseqError::with_context("Failed to read the view columns", e))
}

/// `df`, whose leading columns are those of `schema` with `without_views`
/// applied, with those columns cast back to the types of `schema`.
fn with_views(df: DataFrame, schema: &ArrowSchema) -> Result<DataFrame, LtseqError> {
    let exprs = df
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let column = Expr::Column(datafusion::common::Column::new_unqualified(f.name()));
            match schema.field_with_name(f.name()) {
                Ok(original) if plain_type(original.data_type()).is_some() => {
                    cast(column, original.data_type().clone()).alias(f.name())
                }
                _ => column,
            }
        })
        .collect::<Vec<_>>();
    df.select(exprs)
        .map_err(|e| LtseqError::with_context("Failed to cast view columns back", e))
}

/// Get only the first row of each group of a `group_id_impl` product whose
/// grouping columns are `names`
///
/// It clones a plan that may hold deep expressions, so `lib.rs` runs it
/// through `stack::planned` (#335).
pub fn first_row_impl(table: &LTSeqTable, names: &GroupColumns) -> Result<LTSeqTable, LtseqError> {
    first_or_last_row_impl(table, true, names)
}

/// Get only the last row of each group, as [`first_row_impl`] the first
pub fn last_row_impl(table: &LTSeqTable, names: &GroupColumns) -> Result<LTSeqTable, LtseqError> {
    first_or_last_row_impl(table, false, names)
}

/// Shared implementation for first_row and last_row
fn first_or_last_row_impl(
    table: &LTSeqTable,
    is_first: bool,
    names: &GroupColumns,
) -> Result<LTSeqTable, LtseqError> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }
    let (df, schema) = table.require_df_and_schema()?;

    let has = |name: &str| schema.fields().iter().any(|f| f.name() == name);
    // Verify the group id column exists
    if !has(&names.id) {
        return Err(LtseqError::Validation(format!(
            "{} column not found. Call flatten() first.",
            names.id
        )));
    }

    let has_rn = has(&names.rn);
    let has_count = has(&names.count);

    // Both group_id producers (the native window path and the linear scan)
    // emit __rn__ and __group_count__, so the error arm is unreachable for
    // flatten() products. Guard against direct misuse instead of keeping the
    // old collect → MemTable → SQL fallback alive (issue #91 PR 4).
    let (predicate, which) = match (is_first, has_rn, has_count) {
        (true, true, _) => (column(&names.rn).eq(lit(1i64)), "first"),
        (false, true, true) => (column(&names.rn).eq(column(&names.count)), "last"),
        _ => {
            return Err(LtseqError::Validation(
                "first_row/last_row require __rn__ and __group_count__ from flatten(); \
                 legacy group_id tables without them are no longer supported"
                    .into(),
            ))
        }
    };

    // The rows come back in group order (#196). A flatten() product ends in
    // that order, but this filter sits above its sort, and DataFusion is free
    // to repartition a filter above a sort (#311): the order reached the
    // result only when DataFusion pushed the filter beneath the sort. The
    // result declares no sort_specs, so no root sort restores it either.
    // Where the filter is pushed down, the input is already in this order and
    // DataFusion plans no second sort.
    let picked = (**df)
        .clone()
        .filter(predicate)
        .and_then(|df| df.sort(original_order(names)))
        .map_err(|e| LtseqError::Runtime(format!("Failed to filter {} rows: {}", which, e)))?;

    // Drop the grouping columns by exact name (issue #214): a user column
    // such as `__rn_x` is not one of them, nor is a user column `__rn__`
    // when the grouping took another name (#346).
    let keep_cols = original_column_exprs(picked.schema().as_arrow(), names);

    // If no user columns remain (metadata-only table from linear scan),
    // return the filtered DF as-is — count() still works on it.
    let result = if keep_cols.is_empty() {
        picked
    } else {
        picked.select(keep_cols).map_err(|e| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::create_session_context;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};

    /// A directory and a path that names nothing are fallbacks; a lookup
    /// that fails otherwise is an `Io` error naming the path, never a
    /// fallback (R3-F1 of #422's review).
    #[test]
    fn names_one_file_falls_back_only_for_what_the_reader_does_not_open() {
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        assert!(!names_one_file(dir, |p| std::fs::metadata(p)).unwrap());
        let missing = format!("{dir}/ltseq-no-such-file-*.parquet");
        assert!(!names_one_file(&missing, |p| std::fs::metadata(p)).unwrap());
        let exe = std::env::current_exe().unwrap();
        assert!(names_one_file(exe.to_str().unwrap(), |p| std::fs::metadata(p)).unwrap());

        for kind in [std::io::ErrorKind::PermissionDenied, std::io::ErrorKind::Other] {
            let err = names_one_file("events.parquet", |_| Err(std::io::Error::from(kind)))
                .unwrap_err();
            assert!(matches!(err, LtseqError::Io { .. }), "{err:?}");
            assert!(err.to_string().contains("'events.parquet'"), "{err}");
            assert!(std::error::Error::source(&err).is_some());
        }
    }

    /// A table with the columns of a `flatten()` product, its rows in reverse
    /// group order: groups 3, 2 and 1, each in row order. `v` is ten times
    /// the group id plus the row number.
    fn flattened_in_reverse_group_order() -> LTSeqTable {
        let groups: Vec<i64> = vec![3, 3, 2, 1, 1, 1];
        let counts: Vec<i64> = vec![2, 2, 1, 3, 3, 3];
        let rns: Vec<i64> = vec![1, 2, 1, 1, 2, 3];
        let values = groups.iter().zip(&rns).map(|(g, r)| g * 10 + r).collect();
        let schema = Arc::new(ArrowSchema::new(
            ["v", "__group_id__", "__group_count__", "__rn__"]
                .map(|name| Field::new(name, DataType::Int64, false))
                .to_vec(),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            [values, groups, counts, rns]
                .into_iter()
                .map(|column| Arc::new(Int64Array::from(column)) as _)
                .collect(),
        )
        .expect("valid batch");
        LTSeqTable::from_batches(create_session_context(), vec![batch], schema, Vec::new())
            .expect("table")
    }

    /// `v` of the rows a terminal reads from `table`, in the order it reads them.
    fn values(table: &LTSeqTable) -> Vec<i64> {
        let df = table.ordered_df().expect("plan");
        RUNTIME
            .block_on(df.collect())
            .expect("collect")
            .iter()
            .flat_map(|b| {
                b.column_by_name("v")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .expect("v column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// #196: first_row and last_row put their rows in group order
    /// themselves. A `flatten()` product ends with that order, but their
    /// filter sits above it, so the order reached their result only when
    /// DataFusion pushed the filter beneath the sort.
    #[test]
    fn first_and_last_rows_come_back_in_group_order() {
        let table = flattened_in_reverse_group_order();
        let names = GroupColumns::default();
        let first = first_row_impl(&table, &names).expect("first_row");
        let last = last_row_impl(&table, &names).expect("last_row");
        assert_eq!(values(&first), vec![11, 21, 31]);
        assert_eq!(values(&last), vec![13, 21, 32]);
    }
}
