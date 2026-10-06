//! The places a literal meets a value of known type, shared by the row,
//! window and group dialects: binary operands, `is_in` items, the values of
//! `if_else`/`coalesce`/`fill_null`, `shift` defaults and the other operand
//! of `dt.diff`.
//!
//! Each place asks the resolver for the type of the value the literal meets
//! (for values, the type DataFusion gives the other values together), asks
//! [`interpret`] how to read the literal next to it, and puts that reading
//! in place. When the resolver cannot type the value, the literal is left
//! alone and DataFusion reports the problem.

use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{case, Expr};
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;

use super::literal_policy::{interpret, Position};
use super::Resolver;

/// How errors name the value a literal is read against.
fn describe(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => format!("column '{}'", column.name),
        Expr::Alias(alias) => describe(&alias.expr),
        _ => "the expression".to_string(),
    }
}

/// `literal`, read next to `value`: the literal to use instead, or `None`.
fn read(
    literal: &ScalarValue,
    value: &Expr,
    position: Position,
    rx: &Resolver<'_>,
) -> Result<Option<ScalarValue>, String> {
    match rx.data_type(value) {
        Ok(context) => interpret(literal, &context, position, &describe(value)),
        Err(_) => Ok(None),
    }
}

/// The operands of a comparison or arithmetic operator, with a literal on
/// either side read next to the other one. Two literals are left to
/// DataFusion.
pub(crate) fn binary_operands(
    left: Expr,
    right: Expr,
    position: Position,
    rx: &Resolver<'_>,
) -> Result<(Expr, Expr), String> {
    match (rx.literal(&left), rx.literal(&right)) {
        (Some(literal), None) => {
            let left = read(&literal, &right, position, rx)?.map_or(left, lit);
            Ok((left, right))
        }
        (None, Some(literal)) => {
            let right = read(&literal, &left, position, rx)?.map_or(right, lit);
            Ok((left, right))
        }
        _ => Ok((left, right)),
    }
}

/// `expr IN (list)` with each literal item read next to `expr`.
pub(crate) fn in_list(expr: Expr, list: Vec<Expr>, rx: &Resolver<'_>) -> Result<Expr, String> {
    let list = list
        .into_iter()
        .map(|item| match rx.literal(&item) {
            Some(literal) => Ok(read(&literal, &expr, Position::Comparison, rx)?.map_or(item, lit)),
            None => Ok(item),
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(expr.in_list(list, false))
}

/// Values that share a result column, each literal read next to the type
/// DataFusion gives the other values together: `unify` builds the
/// expression that combines them (a CASE, a coalesce), and the other values'
/// type is that expression's type with every literal replaced by NULL.
fn values(
    values: Vec<Expr>,
    unify: impl Fn(Vec<Expr>) -> Result<Expr, String>,
    rx: &Resolver<'_>,
) -> Result<Vec<Expr>, String> {
    let literals: Vec<Option<ScalarValue>> = values.iter().map(|value| rx.literal(value)).collect();
    let others: Vec<&Expr> = values
        .iter()
        .zip(&literals)
        .filter_map(|(value, literal)| literal.is_none().then_some(value))
        .collect();
    if others.is_empty() || others.len() == values.len() {
        return Ok(values);
    }
    let without_literals = values
        .iter()
        .zip(&literals)
        .map(|(value, literal)| match literal {
            Some(_) => lit(ScalarValue::Null),
            None => value.clone(),
        })
        .collect();
    let Ok(context) = unify(without_literals).and_then(|e| rx.data_type(&e)) else {
        return Ok(values);
    };
    let name = match others.as_slice() {
        [one] => describe(one),
        _ => "the values next to it".to_string(),
    };
    values
        .into_iter()
        .zip(literals)
        .map(|(value, literal)| match literal {
            Some(literal) => {
                Ok(interpret(&literal, &context, Position::Value, &name)?.map_or(value, lit))
            }
            None => Ok(value),
        })
        .collect()
}

/// `CASE WHEN cond THEN true_expr ELSE false_expr END` (`if_else`), a literal
/// branch read next to the other branch.
pub(crate) fn if_else(
    cond: Expr,
    true_expr: Expr,
    false_expr: Expr,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    let build = |cond: Expr, branches: Vec<Expr>| -> Result<Expr, String> {
        let [true_expr, false_expr]: [Expr; 2] = branches
            .try_into()
            .map_err(|_| "if_else has two branches".to_string())?;
        case(cond)
            .when(lit(true), true_expr)
            .otherwise(false_expr)
            .map_err(|e| format!("Failed to create CASE expression: {e}"))
    };
    let branches = values(
        vec![true_expr, false_expr],
        |branches| build(cond.clone(), branches),
        rx,
    )?;
    build(cond, branches)
}

/// `coalesce(values)` (also `fill_null`), each literal read next to the
/// other values.
pub(crate) fn coalesce_values(args: Vec<Expr>, rx: &Resolver<'_>) -> Result<Expr, String> {
    Ok(coalesce(values(args, |args| Ok(coalesce(args)), rx)?))
}

/// A `shift` default, read next to the shifted values.
pub(crate) fn shift_default(
    column: &Expr,
    default: ScalarValue,
    rx: &Resolver<'_>,
) -> Result<ScalarValue, String> {
    if default.is_null() {
        return Ok(default);
    }
    Ok(read(&default, column, Position::Value, rx)?.unwrap_or(default))
}

/// The other operand of `dt.diff`, read next to the receiver like an
/// arithmetic operand (decision D-d).
pub(crate) fn dt_diff_other(on: &Expr, other: Expr, rx: &Resolver<'_>) -> Result<Expr, String> {
    match rx.literal(&other) {
        Some(literal) => Ok(read(&literal, on, Position::Arithmetic, rx)?.map_or(other, lit)),
        None => Ok(other),
    }
}
