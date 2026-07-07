//! Native group-window operations for NestedTable derive/filter (issue #91 PR 4).
//!
//! NestedTable's derive and filter used to arrive as Python-generated SQL
//! strings executed through a collect → MemTable → SQL round-trip. They now
//! arrive as serialized group-dialect dicts and are planned as native
//! DataFusion window expressions over the internal grouping columns that
//! `group_id_impl` produces: `__group_id__` (partition key) and `__rn__`
//! (in-group row order). The plan stays lazy throughout.
//!
//! Group dialect nodes (BinOp/UnaryOp/Literal reuse the row-dialect shapes):
//! - `{"type": "GroupCount"}`
//! - `{"type": "GroupAgg", "func": "max|min|sum|avg|mean|median|std|var|percentile", "column": c, "arg": p?}`
//! - `{"type": "GroupRowColumn", "row": "first|last", "column": c}`
//! - `{"type": "GroupQuantifier", "quant": "all|any|none", "pred": <row-dialect dict>}`

use crate::error::LtseqError;
use crate::transpiler::window_native::aggregate_to_window;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn as agg_fn;
use datafusion::functions_window::expr_fn::{first_value, last_value};
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{
    case, BinaryExpr, Expr, ExprFunctionExt, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Internal grouping columns produced by `group_id_impl` / linear scan.
const INTERNAL_COLS: [&str; 3] = ["__group_id__", "__group_count__", "__rn__"];

enum RowPick {
    First,
    Last,
}

enum Quant {
    All,
    Any,
    None_,
}

/// One node of the serialized group dialect.
enum GroupNode {
    Count,
    Agg { func: String, column: String, arg: Option<f64> },
    RowColumn { row: RowPick, column: String },
    Quantifier { quant: Quant, pred: PyExpr },
    BinOp { op: String, left: Box<GroupNode>, right: Box<GroupNode> },
    UnaryOp { op: String, operand: Box<GroupNode> },
    Literal(PyExpr),
}

fn dict_to_group_node(dict: &Bound<'_, PyDict>) -> PyResult<GroupNode> {
    let node_type: String = dict
        .get_item("type")?
        .ok_or_else(|| LtseqError::Validation("Group expression missing 'type'".into()))?
        .extract()
        .map_err(|_| LtseqError::TypeMismatch("Group expression 'type' must be string".into()))?;

    let get_str = |key: &str| -> PyResult<String> {
        dict.get_item(key)?
            .ok_or_else(|| {
                PyErr::from(LtseqError::Validation(format!(
                    "Group node '{}' missing '{}'",
                    node_type, key
                )))
            })?
            .extract()
            .map_err(|_| {
                PyErr::from(LtseqError::TypeMismatch(format!(
                    "Group node field '{}' must be string",
                    key
                )))
            })
    };

    let get_dict = |key: &str| -> PyResult<Bound<'_, PyDict>> {
        dict.get_item(key)?
            .ok_or_else(|| {
                PyErr::from(LtseqError::Validation(format!(
                    "Group node '{}' missing '{}'",
                    node_type, key
                )))
            })?
            .cast_into::<PyDict>()
            .map_err(|_| {
                PyErr::from(LtseqError::TypeMismatch(format!(
                    "Group node field '{}' must be dict",
                    key
                )))
            })
    };

    Ok(match node_type.as_str() {
        "GroupCount" => GroupNode::Count,
        "GroupAgg" => GroupNode::Agg {
            func: get_str("func")?,
            column: get_str("column")?,
            arg: dict
                .get_item("arg")?
                .and_then(|v| v.extract::<f64>().ok()),
        },
        "GroupRowColumn" => GroupNode::RowColumn {
            row: match get_str("row")?.as_str() {
                "first" => RowPick::First,
                "last" => RowPick::Last,
                other => {
                    return Err(LtseqError::Validation(format!(
                        "GroupRowColumn 'row' must be first|last, got '{}'",
                        other
                    ))
                    .into())
                }
            },
            column: get_str("column")?,
        },
        "GroupQuantifier" => GroupNode::Quantifier {
            quant: match get_str("quant")?.as_str() {
                "all" => Quant::All,
                "any" => Quant::Any,
                "none" => Quant::None_,
                other => {
                    return Err(LtseqError::Validation(format!(
                        "GroupQuantifier 'quant' must be all|any|none, got '{}'",
                        other
                    ))
                    .into())
                }
            },
            pred: dict_to_py_expr(&get_dict("pred")?)?,
        },
        "BinOp" => GroupNode::BinOp {
            op: get_str("op")?,
            left: Box::new(dict_to_group_node(&get_dict("left")?)?),
            right: Box::new(dict_to_group_node(&get_dict("right")?)?),
        },
        "UnaryOp" => GroupNode::UnaryOp {
            op: get_str("op")?,
            operand: Box::new(dict_to_group_node(&get_dict("operand")?)?),
        },
        // Row-dialect literal: let the existing parser handle value/dtype.
        "Literal" => GroupNode::Literal(dict_to_py_expr(dict)?),
        other => {
            return Err(LtseqError::Validation(format!(
                "Unknown group expression type: '{}'",
                other
            ))
            .into())
        }
    })
}

/// `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` — every row in
/// the partition sees the whole group.
fn full_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::Null),
        WindowFrameBound::Following(ScalarValue::Null),
    )
}

fn group_partition() -> Vec<Expr> {
    vec![Expr::Column(Column::new_unqualified("__group_id__"))]
}

fn rn_order() -> Vec<Sort> {
    vec![Sort::new(
        Expr::Column(Column::new_unqualified("__rn__")),
        true,
        false,
    )]
}

/// Convert a group-dialect node into a native DataFusion expression over the
/// internal grouping columns.
fn group_node_to_expr(node: GroupNode, schema: &ArrowSchema) -> Result<Expr, String> {
    match node {
        GroupNode::Count => aggregate_to_window(
            agg_fn::count(lit(1i64)),
            group_partition(),
            vec![],
            full_frame(),
        ),
        GroupNode::Agg { func, column, arg } => {
            let col_expr = Expr::Column(Column::new_unqualified(&column));
            let agg = match func.as_str() {
                "max" => agg_fn::max(col_expr),
                "min" => agg_fn::min(col_expr),
                "sum" => agg_fn::sum(col_expr),
                "avg" | "mean" => agg_fn::avg(col_expr),
                "median" => agg_fn::median(col_expr),
                "stddev" | "std" => agg_fn::stddev(col_expr),
                "variance" | "var" => agg_fn::var_sample(col_expr),
                "percentile" => {
                    let p = arg.unwrap_or(0.5);
                    let sort = Sort::new(col_expr, true, false);
                    agg_fn::approx_percentile_cont(sort, lit(p), None)
                }
                other => return Err(format!("Unknown group aggregate: '{}'", other)),
            };
            aggregate_to_window(agg, group_partition(), vec![], full_frame())
        }
        GroupNode::RowColumn { row, column } => {
            let col_expr = Expr::Column(Column::new_unqualified(&column));
            match row {
                // FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)
                // — default frame (up to CURRENT ROW) suffices for first_value.
                RowPick::First => first_value(col_expr)
                    .partition_by(group_partition())
                    .order_by(rn_order())
                    .build()
                    .map_err(|e| format!("Failed to build first_value window: {}", e)),
                // LAST_VALUE needs the full frame, or each row would see
                // itself as "last" (SQL default frame ends at CURRENT ROW).
                RowPick::Last => last_value(col_expr)
                    .partition_by(group_partition())
                    .order_by(rn_order())
                    .window_frame(full_frame())
                    .build()
                    .map_err(|e| format!("Failed to build last_value window: {}", e)),
            }
        }
        GroupNode::Quantifier { quant, pred } => {
            if crate::transpiler::contains_window_function(&pred) {
                return Err(
                    "Quantifier predicates cannot contain window functions (shift/diff/rolling/cum_sum)"
                        .to_string(),
                );
            }
            let inner = crate::transpiler::pyexpr_to_datafusion(pred, schema)?;
            let case_expr = case(inner)
                .when(lit(true), lit(1i64))
                .otherwise(lit(0i64))
                .map_err(|e| format!("Failed to build quantifier CASE: {}", e))?;
            // all  → MIN(CASE...) OVER (...) = 1  (no row failed)
            // any  → MAX(CASE...) OVER (...) = 1  (some row passed)
            // none → MAX(CASE...) OVER (...) = 0  (no row passed)
            let (agg, expected) = match quant {
                Quant::All => (agg_fn::min(case_expr), 1i64),
                Quant::Any => (agg_fn::max(case_expr), 1i64),
                Quant::None_ => (agg_fn::max(case_expr), 0i64),
            };
            let window = aggregate_to_window(agg, group_partition(), vec![], full_frame())?;
            Ok(window.eq(lit(expected)))
        }
        GroupNode::BinOp { op, left, right } => {
            let left_expr = group_node_to_expr(*left, schema)?;
            let right_expr = group_node_to_expr(*right, schema)?;
            let operator = crate::transpiler::op_str_to_operator(&op)?;
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                operator,
                Box::new(right_expr),
            )))
        }
        GroupNode::UnaryOp { op, operand } => {
            let operand_expr = group_node_to_expr(*operand, schema)?;
            match op.as_str() {
                "Not" => Ok(Expr::Not(Box::new(operand_expr))),
                other => Err(format!("Unknown group unary operator: '{}'", other)),
            }
        }
        GroupNode::Literal(py_expr) => crate::transpiler::pyexpr_to_datafusion(py_expr, schema),
    }
}

/// Original (user-visible) columns: everything except the internal grouping
/// columns. Inputs to these ops are always `flatten()` products, so exact-name
/// exclusion is sound.
fn original_column_exprs(schema: &ArrowSchema) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .filter(|f| !INTERNAL_COLS.contains(&f.name().as_str()))
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect()
}

/// Restore the original row order after window evaluation: DataFusion's
/// window exec re-partitions rows by the PARTITION BY key, so the output
/// order is otherwise unspecified. (__group_id__, __rn__) ascending is by
/// construction the input order of the flattened table.
fn original_order() -> Vec<datafusion::logical_expr::SortExpr> {
    vec![
        datafusion::logical_expr::SortExpr::new(
            Expr::Column(Column::new_unqualified("__group_id__")),
            true,
            false,
        ),
        datafusion::logical_expr::SortExpr::new(
            Expr::Column(Column::new_unqualified("__rn__")),
            true,
            false,
        ),
    ]
}

fn require_grouping_columns(schema: &ArrowSchema) -> PyResult<()> {
    for required in ["__group_id__", "__rn__"] {
        if schema.field_with_name(required).is_err() {
            return Err(LtseqError::Validation(format!(
                "Group-window operations require '{}' from flatten(); \
                 call this on a flattened NestedTable",
                required
            ))
            .into());
        }
    }
    Ok(())
}

/// NestedTable.derive(): add group-window derived columns in one lazy
/// projection. Output = original columns (internals stripped, including
/// `__group_count__`, which the legacy SQL path leaked) + derived columns.
pub fn derive_group_window_impl(
    table: &LTSeqTable,
    derived: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;
    require_grouping_columns(schema)?;

    // Stage A keeps every current column (the internals are needed to restore
    // row order afterwards) and appends the derived window columns.
    let mut stage_a: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    let mut final_names: Vec<String> = Vec::new();
    for (col_name, expr_item) in derived.iter() {
        let name: String = col_name
            .extract()
            .map_err(|_| LtseqError::TypeMismatch("Column name must be string".into()))?;
        let expr_dict = expr_item
            .cast::<PyDict>()
            .map_err(|_| LtseqError::TypeMismatch("Group expression must be dict".into()))?;
        let node = dict_to_group_node(&expr_dict)?;
        let expr = group_node_to_expr(node, schema).map_err(LtseqError::Transpile)?;
        stage_a.push(expr.alias(&name));
        final_names.push(name);
    }

    let mut final_exprs = original_column_exprs(schema);
    final_exprs.extend(
        final_names
            .iter()
            .map(|n| Expr::Column(Column::new_unqualified(n))),
    );

    let result_df = (**df)
        .clone()
        .select(stage_a)
        .and_then(|df| df.sort(original_order()))
        .and_then(|df| df.select(final_exprs))
        .map_err(|e| LtseqError::Runtime(format!("Group derive failed: {}", e)))?;

    // Row order is preserved by the projection, so the declared sort survives.
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_specs.clone(),
        None,
    ))
}

/// NestedTable.filter(): staged native filter on a group-level predicate.
///
/// Stage A: project all columns + predicate as hidden `__group_filter__`
/// Stage B: filter on it
/// Stage C: project the original columns (drops the hidden + internal cols)
pub fn filter_group_window_impl(
    table: &LTSeqTable,
    predicate: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;
    require_grouping_columns(schema)?;

    let node = dict_to_group_node(predicate)?;
    let pred_expr = group_node_to_expr(node, schema).map_err(LtseqError::Transpile)?;

    // Stage A: all current columns + the predicate as a hidden column.
    let mut stage_a: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    stage_a.push(pred_expr.alias("__group_filter__"));

    let result_df = (**df)
        .clone()
        .select(stage_a)
        .and_then(|df| df.filter(col("__group_filter__")))
        .and_then(|df| df.sort(original_order()))
        .and_then(|df| df.select(original_column_exprs(schema)))
        .map_err(|e| LtseqError::Runtime(format!("Group filter failed: {}", e)))?;

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_specs.clone(),
        None,
    ))
}
