//! The optimizer rule that splits the zeros out of the float IN lists
//! DataFusion builds itself (#245).
//!
//! DataFusion compares the items of a float IN list bit for bit, while its
//! `=` takes -0.0 to equal 0.0. Lowering builds no float list that holds a
//! zero, and compares a float with a value that may fold into one as a
//! range (`literals::membership`, `literals::float_equality`). A zero can
//! still reach a list later, through a value lowering sees only as a
//! column: `push_down_filter` replaces a column a projection computes from
//! a constant with that constant, so `r.x == r.z` on a derived `z = -0.0`
//! becomes `x = -0.0`, and the simplifier merges it with the equalities
//! beside it into `x IN (..., -0.0)`. This rule is the last of each
//! optimizer pass, so the plan the optimizer returns holds no such list.
//!
//! It cannot undo what the simplifier did to such a list before: two lists
//! on one value are intersected and subtracted comparing their items as
//! expressions, so `0.0` and `-0.0` differ there (#296).

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;

use super::literals::{signed_zeros, within};

/// Splits each float IN list of a plan that holds a zero ([`split`]).
/// Registered after DataFusion's own rules.
#[derive(Debug, Default)]
pub(crate) struct FloatListZeros;

impl OptimizerRule for FloatListZeros {
    fn name(&self) -> &str {
        "ltseq_float_list_zeros"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // A projection's columns are named after their expressions.
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            if !expr.exists(|e| Ok(list_zeros(e).is_some()))? {
                return Ok(Transformed::no(expr));
            }
            let name = names.save(&expr);
            Ok(expr
                .transform_up(split)?
                .update_data(|expr| name.restore(expr)))
        })
    }
}

/// -0.0 and 0.0, encoded as the list's items are, when `expr` is an IN
/// list holding a float zero.
fn list_zeros(expr: &Expr) -> Option<(ScalarValue, ScalarValue)> {
    match expr {
        Expr::InList(InList { list, .. }) => list.iter().find_map(item_zeros),
        _ => None,
    }
}

/// -0.0 and 0.0, encoded as `item` is, when `item` is a float zero.
fn item_zeros(item: &Expr) -> Option<(ScalarValue, ScalarValue)> {
    match item {
        Expr::Literal(value, _) => signed_zeros(value),
        _ => None,
    }
}

/// `value IN (items)` holding a float zero as lowering builds it: the list
/// without its zeros, ORed with the range of both zeros ([`within`]). The
/// analyzer has coerced the value and the items to one type, which the
/// bounds keep, since no coercion follows this rule.
fn split(expr: Expr) -> Result<Transformed<Expr>> {
    let Some((negative, zero)) = list_zeros(&expr) else {
        return Ok(Transformed::no(expr));
    };
    let Expr::InList(InList {
        expr: value,
        list,
        negated,
    }) = expr
    else {
        unreachable!("only an IN list holds zeros");
    };
    let rest: Vec<Expr> = list
        .into_iter()
        .filter(|item| item_zeros(item).is_none())
        .collect();
    let range = within((*value).clone(), (lit(negative), lit(zero)), false);
    let test = if rest.is_empty() {
        range
    } else {
        value.in_list(rest, false).or(range)
    };
    Ok(Transformed::yes(if negated { !test } else { test }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::col;

    fn split_expr(expr: Expr) -> Expr {
        split(expr).unwrap().data
    }

    #[test]
    fn a_list_holding_a_zero_compares_it_as_a_range() {
        let x = || col("x");
        let range = || x().gt_eq(lit(-0.0)).and(x().lt_eq(lit(0.0)));
        let list = x().in_list(vec![lit(1.0), lit(-0.0), lit(2.0), lit(0.0)], false);
        assert_eq!(
            split_expr(list),
            x().in_list(vec![lit(1.0), lit(2.0)], false).or(range())
        );
        assert_eq!(split_expr(x().in_list(vec![lit(0.0)], false)), range());
        let negated = x().in_list(vec![lit(1.0), lit(0.0)], true);
        assert_eq!(
            split_expr(negated),
            !x().in_list(vec![lit(1.0)], false).or(range())
        );
    }

    #[test]
    fn the_bounds_keep_the_items_encoding() {
        let key = Box::new(DataType::Int32);
        let encoded = |v: f32| {
            lit(ScalarValue::Dictionary(
                key.clone(),
                Box::new(ScalarValue::Float32(Some(v))),
            ))
        };
        let list = col("x").in_list(vec![encoded(1.0), encoded(-0.0)], false);
        let range = col("x")
            .gt_eq(encoded(-0.0))
            .and(col("x").lt_eq(encoded(0.0)));
        assert_eq!(
            split_expr(list),
            col("x").in_list(vec![encoded(1.0)], false).or(range)
        );
    }

    #[test]
    fn other_lists_are_left_alone() {
        for list in [
            col("x").in_list(
                vec![lit(1.0), lit(f64::NAN), lit(ScalarValue::Float64(None))],
                false,
            ),
            col("i").in_list(vec![lit(0i64), lit(1i64)], false),
            col("x").in_list(vec![col("y"), lit(1.0)], false),
        ] {
            let result = split(list.clone()).unwrap();
            assert!(!result.transformed);
            assert_eq!(result.data, list);
        }
    }
}
