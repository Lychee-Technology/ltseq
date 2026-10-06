//! The places a literal meets a value of known type, shared by the row,
//! window and group dialects: binary operands, `is_in` items, the values of
//! `if_else`/`coalesce`/`fill_null`, `shift` defaults and the other operand
//! of `dt.diff`.
//!
//! Each place asks the resolver for the value type of the value the literal
//! meets (for values, the type DataFusion gives the other values together;
//! a dictionary column's value type, never its encoding), asks
//! [`interpret`] how to read the literal next to it, and puts that reading
//! in place. When the resolver cannot type the value, the literal is left
//! alone and DataFusion reports the problem.
//!
//! Comparisons and `is_in` then check DataFusion's coercion of the pair
//! ([`placement`]): where it would cast an exact operand or cast the
//! literal inexactly, the literal is placed among the operand's own values
//! instead. An `is_in` list is checked against its equalities as a whole
//! ([`in_list`]).

use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::{case, BinaryExpr, Expr, Operator};
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;

use super::exact::{cast_loss, exact_cast, place, place_instant, Loss, Placement};
use super::literal_policy::{interpret, literal_text, Position, Reading};
use super::Resolver;

/// How errors name the value a literal is read against.
fn describe(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => format!("column '{}'", column.name),
        Expr::Alias(alias) => describe(&alias.expr),
        _ => "the expression".to_string(),
    }
}

/// How to read `literal` next to `value`. A value the resolver cannot
/// type leaves the literal to DataFusion.
fn read(
    literal: &ScalarValue,
    value: &Expr,
    position: Position,
    rx: &Resolver<'_>,
) -> Result<Reading, String> {
    match rx.value_type(value) {
        Ok(context) => interpret(literal, &context, position, &describe(value)),
        Err(_) => Ok(Reading::Keep),
    }
}

/// The literal a reading outside a comparison puts in place, if any.
fn replacement(reading: Reading) -> Option<ScalarValue> {
    match reading {
        Reading::Keep => None,
        Reading::Value(value) => Some(value),
        Reading::Instant(..) => unreachable!("only a comparison reads a literal as an instant"),
    }
}

/// The operands of an arithmetic operator, with a literal on either side
/// read next to the other one. Two literals are left to DataFusion.
pub(crate) fn arithmetic_operands(
    left: Expr,
    right: Expr,
    rx: &Resolver<'_>,
) -> Result<(Expr, Expr), String> {
    let arithmetic = Position::Arithmetic;
    match (rx.literal(&left), rx.literal(&right)) {
        (Some(literal), None) => {
            let left = replacement(read(&literal, &right, arithmetic, rx)?).map_or(left, lit);
            Ok((left, right))
        }
        (None, Some(literal)) => {
            let right = replacement(read(&literal, &left, arithmetic, rx)?).map_or(right, lit);
            Ok((left, right))
        }
        _ => Ok((left, right)),
    }
}

/// `left op right` for a comparison operator. A literal on either side is
/// read next to the other side, then compared exactly where DataFusion's
/// coercion of the pair would not be (see [`placement`]). Two literals are
/// left to DataFusion.
pub(crate) fn comparison(
    op: Operator,
    left: Expr,
    right: Expr,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    let build = |left: Expr, right: Expr| {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    };
    let literal_on_left = match (rx.literal(&left), rx.literal(&right)) {
        (None, Some(_)) => false,
        (Some(_), None) => true,
        _ => return Ok(build(left, right)),
    };
    let (operand, literal_expr) = if literal_on_left {
        (right, left)
    } else {
        (left, right)
    };
    // As seen from the operand: `L < x` is `x > L`.
    let operand_op = if literal_on_left {
        op.swap().expect("comparison operators swap")
    } else {
        op
    };
    let rebuild = |operand: Expr, literal: Expr| {
        if literal_on_left {
            build(literal, operand)
        } else {
            build(operand, literal)
        }
    };
    let (literal_expr, placed) = match compared(&operand, operand_op, &literal_expr, rx)? {
        (Some(read), placed) => (lit(read), placed),
        (None, placed) => (literal_expr, placed),
    };
    Ok(match placed {
        Some(Placement::Exact(value)) => rebuild(operand, lit(value)),
        Some(placed) => exact_comparison(operand, operand_op, placed),
        None => rebuild(operand, literal_expr),
    })
}

/// The literal `literal` (a literal or a constant) compared with `operand`:
/// the literal to use instead of it, if its reading changes it, and where
/// it is placed among the operand's values, if ltseq decides the comparison.
fn compared(
    operand: &Expr,
    op: Operator,
    literal: &Expr,
    rx: &Resolver<'_>,
) -> Result<(Option<ScalarValue>, Option<Placement>), String> {
    let value = rx.literal(literal).expect("compared with a literal");
    Ok(match read(&value, operand, Position::Comparison, rx)? {
        Reading::Keep => (None, placement(operand, op, &value, rx)),
        Reading::Value(read) => {
            let placed = placement(operand, op, &read, rx);
            (Some(read), placed)
        }
        Reading::Instant(ticks, unit) => {
            let placed = rx
                .value_type(operand)
                .ok()
                .and_then(|operand_type| place_instant(ticks, unit, &operand_type));
            (
                None,
                Some(placed.ok_or("an instant compares only with a timestamp")?),
            )
        }
    })
}

/// `expr IN (list)`: the disjunction of `expr == item`, which is how SQL
/// defines IN. Each equality is built like `==` builds it: a literal item
/// is read next to `expr` and placed among its values ([`compared`]), so an
/// item placed exactly takes `expr`'s type and an item no value of that
/// type equals is dropped. A list left empty is false (NULL for a NULL
/// `expr`).
///
/// DataFusion compares an IN list at one type for every item, while each
/// equality compares at the type of its own pair. The IN is kept as written
/// when its type is every equality's. Otherwise one item would be compared
/// at a type its equality does not use (an Int64 item placed exactly next
/// to a Float64 item: the list's Float64 makes 2^53 equal 2^53 + 1), so the
/// list is split into one IN per comparison type, joined by OR. All types
/// come from DataFusion's coercion of the equality and of the IN.
pub(crate) fn in_list(expr: Expr, list: Vec<Expr>, rx: &Resolver<'_>) -> Result<Expr, String> {
    // Each kept item, with its equality's two sides as DataFusion coerces them.
    let mut kept = Vec::with_capacity(list.len());
    for item in list {
        let item = if rx.literal(&item).is_none() {
            item
        } else {
            match compared(&expr, Operator::Eq, &item, rx)? {
                (_, Some(Placement::Exact(value))) => lit(value),
                (_, Some(Placement::Between(_) | Placement::Beyond(_))) => continue,
                (Some(read), None) => lit(read),
                (None, None) => item,
            }
        };
        let Expr::BinaryExpr(BinaryExpr { left, right, .. }) =
            rx.resolve(expr.clone().eq(item.clone()))
        else {
            return Err("is_in: coercing an equality did not give an equality".to_string());
        };
        kept.push((item, *left, *right));
    }
    if kept.is_empty() {
        return Ok(verdict_unless_null(expr, false));
    }
    let comparison_type = |side: &Expr| rx.value_type(side).ok();
    let written = expr.in_list(kept.iter().map(|(item, ..)| item.clone()).collect(), false);
    let list_type = match rx.resolve(written.clone()) {
        Expr::InList(resolved) => comparison_type(&resolved.expr),
        _ => None,
    };
    if kept
        .iter()
        .all(|(_, left, _)| comparison_type(left) == list_type)
    {
        return Ok(written);
    }
    let mut groups: Vec<(Option<DataType>, Expr, Vec<Expr>)> = Vec::new();
    for (_, left, right) in kept {
        let key = comparison_type(&left);
        match groups.iter_mut().find(|(group, ..)| *group == key) {
            Some((_, _, rights)) => rights.push(right),
            None => groups.push((key, left, vec![right])),
        }
    }
    Ok(groups
        .into_iter()
        .map(|(_, left, rights)| left.in_list(rights, false))
        .reduce(Expr::or)
        .expect("at least one item is kept"))
}

/// Where `operand op literal` must be decided by ltseq rather than by
/// DataFusion's coercion of the pair, the literal placed among the
/// operand's values; `None` to leave the comparison to DataFusion.
///
/// DataFusion compares the two at a common type. For an exact operand (an
/// integer, decimal, date or timestamp) that goes wrong when the common
/// type is a decimal, date or timestamp type the operand must be cast to:
/// the cast may not hold every operand value (the 38-digit decimal clamp,
/// the nanosecond range), and the simplifier moves it onto the literal,
/// which truncates a finer timestamp (#200) and panics on a negative-scale
/// decimal (apache/datafusion#24896). It also goes wrong when the literal
/// does not fit the common type exactly. In both cases the literal is
/// placed among the operand's own values, so the operand is never cast. An
/// integer common type is left alone: the simplifier unwraps integer casts
/// exactly. The value placed is the literal's own, except a float's: that
/// is DataFusion's reading of it at the common type, so a float literal
/// keeps DataFusion's semantics.
fn placement(
    operand: &Expr,
    op: Operator,
    literal: &ScalarValue,
    rx: &Resolver<'_>,
) -> Option<Placement> {
    let operand_type = rx.value_type(operand).ok()?;
    let exact = |t: &DataType| {
        t.is_integer()
            || matches!(
                t,
                DataType::Decimal128(..)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(..)
            )
    };
    if !exact(&operand_type) {
        return None;
    }
    let literal_type = literal.data_type();
    let (common, literal_common) = BinaryTypeCoercer::new(&operand_type, &op, &literal_type)
        .get_input_types()
        .ok()?;
    let operand_cast = common != operand_type && exact(&common) && !common.is_integer();
    let reading = literal.cast_to(&literal_common).ok();
    let read_exactly = reading
        .as_ref()
        .and_then(|read| read.cast_to(&literal_type).ok())
        .is_some_and(|back| &back == literal);
    if !operand_cast && read_exactly {
        return None;
    }
    let value = if literal_type.is_floating() {
        reading?
    } else {
        literal.clone()
    };
    place(&value, &operand_type)
}

/// `operand op literal` for a literal placed strictly between two values of
/// the operand's type or beyond all of them. A NULL operand stays NULL
/// (decision P8 on #145).
fn exact_comparison(operand: Expr, op: Operator, placed: Placement) -> Expr {
    use Operator::{Eq, Gt, GtEq, Lt, LtEq};
    match placed {
        // No value equals the literal; above the floor is above the literal.
        Placement::Between(floor) => match op {
            Gt | GtEq => operand.gt(lit(floor)),
            Lt | LtEq => operand.lt_eq(lit(floor)),
            Eq => verdict_unless_null(operand, false),
            _ => verdict_unless_null(operand, true),
        },
        Placement::Beyond(Ordering::Greater) => {
            verdict_unless_null(operand, matches!(op, Lt | LtEq | Operator::NotEq))
        }
        Placement::Beyond(_) => {
            verdict_unless_null(operand, matches!(op, Gt | GtEq | Operator::NotEq))
        }
        Placement::Exact(_) => unreachable!("an exact placement is an ordinary comparison"),
    }
}

/// `verdict` where `expr` is not null, NULL where it is.
fn verdict_unless_null(expr: Expr, verdict: bool) -> Expr {
    case(expr.is_null())
        .when(lit(true), lit(ScalarValue::Boolean(None)))
        .otherwise(lit(verdict))
        .expect("a CASE with one branch and an ELSE builds")
}

/// Values that share a result column (the branches of a CASE, the arguments
/// of a coalesce), with each literal typed exactly next to the others
/// (decision D-b on #225). `unify` builds the expression that combines them.
///
/// The context is the type DataFusion gives the values that are not
/// literals: `unify` of the values with every literal replaced by NULL.
/// Each literal is first read next to it (`interpret`). Then:
///
/// 1. DataFusion unifies the values with the literals as they are. That is
///    kept when it is exact: every literal reaches the result type
///    unchanged (`exact_cast`) and the result type loses nothing of the
///    context type (`cast_loss`), so the literal can widen a decimal to
///    hold its digits.
/// 2. Otherwise each literal the context type holds exactly takes that
///    type, and DataFusion unifies again. A literal that does not reach
///    the result type, or a result type that loses precision against the
///    context type, is now a planning error naming the literal. A result
///    type that loses only range is kept when both are timestamps (a finer
///    literal widens the unit, as DataFusion does) and refused for a date
///    context, whose dates a nanosecond timestamp cannot all hold.
///
/// Losses that the context itself has against the other values (two
/// columns of different types) are DataFusion's, and are left as they are.
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
    let with = |literals: &[Option<ScalarValue>], null: bool| -> Vec<Expr> {
        values
            .iter()
            .zip(literals)
            .map(|(value, literal)| match literal {
                Some(_) if null => lit(ScalarValue::Null),
                Some(literal) => lit(literal.clone()),
                None => value.clone(),
            })
            .collect()
    };
    let unified_type = |values: Vec<Expr>| unify(values).and_then(|e| rx.value_type(&e));
    let Ok(context) = unified_type(with(&literals, true)) else {
        return Ok(values);
    };
    let name = match others.as_slice() {
        [one] => describe(one),
        [init @ .., last] => format!(
            "the common type of {} and {}",
            init.iter()
                .map(|e| describe(e))
                .collect::<Vec<_>>()
                .join(", "),
            describe(last)
        ),
        [] => unreachable!("checked above"),
    };
    // Read each literal next to the context.
    let mut literals = literals
        .into_iter()
        .map(|literal| match literal {
            Some(literal) => Ok(Some(
                replacement(interpret(&literal, &context, Position::Value, &name)?)
                    .unwrap_or(literal),
            )),
            None => Ok(None),
        })
        .collect::<Result<Vec<_>, String>>()?;
    // 1. DataFusion's unification, when it is exact.
    let exact = |literals: &[Option<ScalarValue>], result: &DataType| {
        cast_loss(&context, result) == Loss::None
            && literals
                .iter()
                .flatten()
                .all(|literal| exact_cast(literal, result).is_some())
    };
    let Ok(result) = unified_type(with(&literals, false)) else {
        return Ok(values);
    };
    if !exact(&literals, &result) {
        // 2. Literals the context holds exactly take its type.
        for literal in literals.iter_mut().flatten() {
            if let Some(typed) = exact_cast(literal, &context) {
                *literal = typed;
            }
        }
        let Ok(result) = unified_type(with(&literals, false)) else {
            return Ok(values);
        };
        let refused = literals
            .iter()
            .flatten()
            .find(|literal| {
                literal.data_type() != context && exact_cast(literal, &result).is_none()
            })
            .or_else(|| match cast_loss(&context, &result) {
                Loss::None => None,
                Loss::Range if matches!(context, DataType::Timestamp(..)) => None,
                Loss::Range | Loss::Precision => literals
                    .iter()
                    .flatten()
                    .find(|literal| literal.data_type() != context),
            });
        if let Some(literal) = refused {
            return Err(refusal(literal, &context, &name));
        }
    }
    Ok(values
        .into_iter()
        .zip(literals)
        .map(|(value, literal)| literal.map_or(value, lit))
        .collect())
}

/// The error for a literal that cannot share a result column with values
/// of type `context`.
fn refusal(literal: &ScalarValue, context: &DataType, name: &str) -> String {
    use DataType as T;
    let text = literal_text(literal);
    match (literal, context) {
        (ScalarValue::Decimal128(..), _) => {
            format!("Decimal literal {text} does not fit {name} ({context}) without rounding")
        }
        (_, T::Date32 | T::Date64) if literal.data_type().is_temporal() => {
            format!("{name} is a date; the datetime literal {text} has a time of day; use a date")
        }
        (_, T::Timestamp(..)) if literal.data_type().is_temporal() => {
            format!("{text} is outside the range of {name} ({context})")
        }
        _ => format!("{text} does not fit {name} ({context}) exactly"),
    }
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

/// A `shift` default, read next to the shifted values. `lag`/`lead` cast it
/// to the column's type, so it must survive that cast unchanged
/// (`exact_cast`): a default the column cannot hold exactly is an error
/// rather than a truncated, rounded or overflowing value (decision D-c).
pub(crate) fn shift_default(
    column: &Expr,
    default: ScalarValue,
    rx: &Resolver<'_>,
) -> Result<ScalarValue, String> {
    if default.is_null() {
        return Ok(default);
    }
    // A column of nulls (a header-only CSV) has no type to fit.
    let Some(column_type) = rx.value_type(column).ok().filter(|t| t != &DataType::Null) else {
        return Ok(default);
    };
    let name = describe(column);
    let read =
        replacement(interpret(&default, &column_type, Position::Value, &name)?).unwrap_or(default);
    exact_cast(&read, &column_type).ok_or_else(|| {
        format!(
            "{name} cannot hold the shift() default {} exactly ({column_type})",
            literal_text(&read)
        )
    })
}

/// The other operand of `dt.diff`, read next to the receiver like an
/// arithmetic operand (decision D-d).
pub(crate) fn dt_diff_other(on: &Expr, other: Expr, rx: &Resolver<'_>) -> Result<Expr, String> {
    match rx.literal(&other) {
        Some(literal) => {
            Ok(replacement(read(&literal, on, Position::Arithmetic, rx)?).map_or(other, lit))
        }
        None => Ok(other),
    }
}
