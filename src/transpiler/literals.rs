//! The places a literal meets a value of known type, shared by the row,
//! window and group dialects: binary operands, `is_in` items, the values of
//! `if_else`/`coalesce`/`fill_null`, `shift` defaults and the other operand
//! of `dt.diff`; and, outside expressions, the values `insert` and `update`
//! write into a column.
//!
//! Each place asks the resolver for the value type of the value the literal
//! meets (for values, the type DataFusion gives the other values together;
//! a dictionary column's value type, never its encoding), asks
//! [`interpret`] how to read the literal next to it, and puts that reading
//! in place. When the resolver cannot type the value, the literal is left
//! alone and DataFusion reports the problem.
//!
//! A comparison or `is_in` with an operand of an exact domain (an integer,
//! a decimal, a date or timestamp) places the literal among the operand's
//! own values ([`placement`]), so the operand is never cast and the literal
//! never rounded. An `is_in` list is checked against its equalities as a
//! whole ([`in_list`]). Values that share a result column are typed by
//! [`fit`] and [`widening`] ([`values`]). Every judgement of a value at a
//! type is a fact from `exact`, read as `literal_policy` reads it; nothing
//! here casts a literal to see what happens.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{case, when, BinaryExpr, Case, Expr, Operator};
use datafusion::prelude::{cast, lit};
use datafusion::scalar::ScalarValue;

use super::exact::{hold_instant, Holding, Placement};
use super::literal_policy::{
    exact_domain, fit, held, interpret, literal_text, timestamp_literal, widening, Fit, Position,
    Reading, Widening,
};
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

/// Which side of `left op right` is the literal read next to the other:
/// `Some(true)` for the left. A literal is read next to a value; of two
/// literals (a folded constant, `LiteralExpr(2**53 + 1) > 2.0**53`), the
/// one whose type is of an exact domain is the value, the left when both
/// are, so that a constant reads as the column of its type would (the
/// position parity of decisions D-j and D-l on #225). Two literals of no
/// exact domain are DataFusion's: `None`.
fn literal_side(left: &Expr, right: &Expr, rx: &Resolver<'_>) -> Option<bool> {
    let exact = |expr: &Expr| rx.value_type(expr).is_ok_and(|t| exact_domain(&t));
    match (rx.literal(left), rx.literal(right)) {
        (None, Some(_)) => Some(false),
        (Some(_), None) => Some(true),
        (Some(_), Some(_)) if exact(left) => Some(false),
        (Some(_), Some(_)) if exact(right) => Some(true),
        _ => None,
    }
}

/// The operands of an arithmetic operator, with a literal on either side
/// read next to the other one (see [`literal_side`]).
pub(crate) fn arithmetic_operands(
    left: Expr,
    right: Expr,
    rx: &Resolver<'_>,
) -> Result<(Expr, Expr), String> {
    let arithmetic = Position::Arithmetic;
    match literal_side(&left, &right, rx) {
        Some(true) => {
            let literal = rx.literal(&left).expect("the literal side");
            let left = replacement(read(&literal, &right, arithmetic, rx)?).map_or(left, lit);
            Ok((left, right))
        }
        Some(false) => {
            let literal = rx.literal(&right).expect("the literal side");
            let right = replacement(read(&literal, &left, arithmetic, rx)?).map_or(right, lit);
            Ok((left, right))
        }
        None => Ok((left, right)),
    }
}

/// `left op right` for a comparison operator. A literal on either side is
/// read next to the other side (see [`literal_side`]), then placed among
/// that side's values when it is of an exact domain (see [`placement`]).
/// A float `==` or `!=` with a value DataFusion may fold into a zero is a
/// range ([`float_equality`]).
pub(crate) fn comparison(
    op: Operator,
    left: Expr,
    right: Expr,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    let build = |left: Expr, right: Expr| {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    };
    let Some(literal_on_left) = literal_side(&left, &right, rx) else {
        let equality = query_constant_equality(op, &left, &right, rx)?;
        return Ok(equality.unwrap_or_else(|| build(left, right)));
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
    let (literal_expr, placed) = match compared(&operand, &literal_expr, rx)? {
        (Some(read), placed) => (lit(read), placed),
        (None, placed) => (literal_expr, placed),
    };
    Ok(match placed {
        Holding::Exactly(value) => rebuild(operand, lit(value)),
        Holding::Not(placed) => exact_comparison(operand, operand_op, placed),
        Holding::Unjudged => match float_equality(op, &operand, &literal_expr, rx)? {
            Some(range) => range,
            None => rebuild(operand, literal_expr),
        },
    })
}

/// `left op right` as [`float_equality`] builds it, when one side has one
/// value per query and the other varies by row
/// ([`Resolver::query_constant`]).
fn query_constant_equality(
    op: Operator,
    left: &Expr,
    right: &Expr,
    rx: &Resolver<'_>,
) -> Result<Option<Expr>, String> {
    match (
        Resolver::query_constant(left),
        Resolver::query_constant(right),
    ) {
        (true, false) => float_equality(op, right, left, rx),
        (false, true) => float_equality(op, left, right, rx),
        _ => Ok(None),
    }
}

/// `value == other`, or `value != other` for `NotEq`, when DataFusion
/// compares the two as floats and `other` may fold into a zero; `None`
/// leaves the comparison as written.
///
/// DataFusion's `=` takes -0.0 to equal 0.0, but its simplifier merges
/// `x = a OR x = b OR ...` on a column into one IN list once each `a` is a
/// literal, also after it has made a column of a repeated expression, and
/// negating `x != a` gives `x = a`; the list then decides a zero by its
/// sign ([`membership`]). So such an equality is the range of the floats
/// `=` takes to equal `other` ([`within`]). A constant other than a zero
/// leaves the equality as written. A value that has one value per query
/// but is not a constant (`now()` and what is computed from it) folds only
/// when the query runs, so lowering cannot tell whether it is a zero; its
/// bounds are expressions DataFusion folds with it ([`equal_range`]).
fn float_equality(
    op: Operator,
    value: &Expr,
    other: &Expr,
    rx: &Resolver<'_>,
) -> Result<Option<Expr>, String> {
    let negated = match op {
        Operator::Eq => false,
        Operator::NotEq => true,
        _ => return Ok(None),
    };
    // A pair DataFusion cannot coerce (`bool_col == 0.0`, a float with a
    // Boolean or a timestamp of `now()`) is left for it to reject.
    let Some(Expr::BinaryExpr(BinaryExpr { left, right, .. })) =
        rx.coerced(&value.clone().eq(other.clone()))?
    else {
        return Ok(None);
    };
    let float_type = match (rx.value_type(&left), rx.value_type(&right)) {
        (Ok(left), Ok(right)) if left == right && left.is_floating() => left,
        _ => return Ok(None),
    };
    let bounds = if Resolver::constant(&right) {
        if !rx.literal(&right).is_some_and(|v| is_float_zero(&v)) {
            return Ok(None);
        }
        zero_bounds(&float_type)
    } else if Resolver::query_constant(&right) {
        // The value at the comparison's own type, decoded if coercion
        // encoded it.
        let plain = if rx.data_type(&right).is_ok_and(|t| t == float_type) {
            *right
        } else {
            cast(*right, float_type.clone())
        };
        equal_range(plain, &float_type)
    } else {
        return Ok(None);
    };
    Ok(Some(within(value.clone(), bounds, negated)))
}

/// `low <= value <= high` as `value >= low AND value <= high`, or for
/// `negated` its complement, `value < low OR value > high`.
///
/// This is how a float is compared with a zero. DataFusion's comparisons
/// normalize zeros and then order floats by IEEE 754 totalOrder, which
/// puts -0.0 just below 0.0, so the range from -0.0 to 0.0 holds both
/// zeros and nothing else, also where the zeros are not normalized (an
/// encoded column, #292); it holds no NaN, as `=` does not take a NaN to
/// equal a zero. DataFusion neither merges a range into an IN list nor
/// unfolds it into `=`, so it never meets the simplifier's intersection
/// and difference of lists, which lose NULL results (#296).
fn within(value: Expr, (low, high): (Expr, Expr), negated: bool) -> Expr {
    if negated {
        value.clone().lt(low).or(value.gt(high))
    } else {
        value.clone().gt_eq(low).and(value.lt_eq(high))
    }
}

/// The least and the greatest float DataFusion's `=` takes to equal the
/// float `value`: -0.0 and 0.0 where `value` is a zero, else `value` both
/// times. DataFusion folds both with `value`.
fn equal_range(value: Expr, float_type: &DataType) -> (Expr, Expr) {
    let (negative, zero) = zero_bounds(float_type);
    let bound = |bound: Expr| {
        when(value.clone().eq(zero.clone()), bound)
            .otherwise(value.clone())
            .expect("a CASE with one branch and an ELSE builds")
    };
    (bound(negative), bound(zero.clone()))
}

/// -0.0 and 0.0 of the float type `float_type`, the bounds of the range a
/// zero is compared as ([`within`]).
fn zero_bounds(float_type: &DataType) -> (Expr, Expr) {
    let zero = ScalarValue::new_zero(float_type).expect("a float type has a zero");
    let negative = zero.arithmetic_negate().expect("a float negates");
    (lit(negative), lit(zero))
}

/// The literal `literal` (a literal or a constant) compared with `operand`:
/// the literal to use instead of it, if its reading changes it, and where
/// it is placed among the operand's values, if ltseq decides the comparison
/// (`Unjudged` leaves it to DataFusion).
fn compared(
    operand: &Expr,
    literal: &Expr,
    rx: &Resolver<'_>,
) -> Result<(Option<ScalarValue>, Holding), String> {
    let value = rx.literal(literal).expect("compared with a literal");
    Ok(match read(&value, operand, Position::Comparison, rx)? {
        Reading::Keep => (None, placement(operand, &value, rx)),
        Reading::Value(read) => {
            let placed = placement(operand, &read, rx);
            (Some(read), placed)
        }
        Reading::Instant(ticks, unit) => {
            let placed = rx
                .value_type(operand)
                .map_or(Holding::Unjudged, |operand_type| {
                    hold_instant(ticks, unit, &operand_type)
                });
            if matches!(placed, Holding::Unjudged) {
                return Err("an instant compares only with a timestamp".to_string());
            }
            (None, placed)
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
/// come from DataFusion's coercion of the equality and of the IN. Each IN
/// is built by [`membership`], which keeps a float list from deciding a
/// zero by its sign.
pub(crate) fn in_list(expr: Expr, list: Vec<Expr>, rx: &Resolver<'_>) -> Result<Expr, String> {
    // Each kept item, with its equality's two sides as DataFusion coerces them.
    let mut kept = Vec::with_capacity(list.len());
    for item in list {
        let item = if rx.literal(&item).is_none() {
            item
        } else {
            match compared(&expr, &item, rx)? {
                (_, Holding::Exactly(value)) => lit(value),
                (_, Holding::Not(_)) => continue,
                (Some(read), Holding::Unjudged) => lit(read),
                (None, Holding::Unjudged) => item,
            }
        };
        let Expr::BinaryExpr(BinaryExpr { left, right, .. }) =
            rx.resolve(expr.clone().eq(item.clone()))?
        else {
            return Err("is_in: coercing an equality did not give an equality".to_string());
        };
        kept.push((item, *left, *right));
    }
    if kept.is_empty() {
        return Ok(verdict_unless_null(expr, false));
    }
    let comparison_type = |side: &Expr| rx.value_type(side).ok();
    let written = expr
        .clone()
        .in_list(kept.iter().map(|(item, ..)| item.clone()).collect(), false);
    let list_type = match rx.resolve(written)? {
        Expr::InList(resolved) => comparison_type(&resolved.expr),
        _ => None,
    };
    let as_written = kept
        .iter()
        .all(|(_, left, _)| comparison_type(left) == list_type);
    // One membership test per comparison type, of `expr` and the items as
    // written when the list's type is every equality's, else of both sides
    // as each equality coerces them; each item with its coerced form.
    let mut groups: Vec<(Option<DataType>, Expr, Vec<ListItem>)> = Vec::new();
    for (item, left, right) in kept {
        let key = comparison_type(&left);
        let (value, item) = if as_written {
            (expr.clone(), item)
        } else {
            (left, right.clone())
        };
        match groups.iter_mut().find(|(group, ..)| *group == key) {
            Some((_, _, items)) => items.push((item, right)),
            None => groups.push((key, value, vec![(item, right)])),
        }
    }
    let tests = groups
        .into_iter()
        .map(|(key, value, items)| membership(key.as_ref(), value, items, rx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(tests
        .into_iter()
        .reduce(Expr::or)
        .expect("at least one item is kept"))
}

/// A list item as it is placed in the list, and as DataFusion coerces it
/// to the comparison type.
type ListItem = (Expr, Expr);

/// `value IN (items)`, compared at `comparison_type`; each item is paired
/// with its form coerced to that type.
///
/// DataFusion compares the items of a float IN list bit for bit: a constant
/// list through a hash set or a branchless filter, any other through
/// Arrow's total-order `eq`. Its `=` compares -0.0 equal to 0.0, as IEEE
/// 754 does, so the list alone would decide a zero by its sign (#245).
/// A float list therefore holds no zero: a zero item is the range of both
/// zeros beside the list ([`within`]), as a zero is for `==`, and an item
/// that is not a constant is compared beside the list as `==` compares it
/// ([`float_equality`]). NaN is left as both compare it: equal to a NaN of
/// the same bits.
fn membership(
    comparison_type: Option<&DataType>,
    value: Expr,
    items: Vec<ListItem>,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    let Some(float_type) = comparison_type.filter(|t| t.is_floating()) else {
        let list = items.into_iter().map(|(item, _)| item).collect();
        return Ok(value.in_list(list, false));
    };
    let mut list = Vec::with_capacity(items.len());
    let mut zero = false;
    let mut equalities = Vec::new();
    for (item, coerced) in items {
        if !Resolver::constant(&coerced) {
            let equality = query_constant_equality(Operator::Eq, &value, &item, rx)?;
            equalities.push(equality.unwrap_or_else(|| value.clone().eq(item)));
        } else if rx.literal(&coerced).is_some_and(|v| is_float_zero(&v)) {
            zero = true;
        } else {
            list.push(item);
        }
    }
    let zeros = zero.then(|| within(value.clone(), zero_bounds(float_type), false));
    let list = (!list.is_empty()).then(|| value.clone().in_list(list, false));
    Ok(list
        .into_iter()
        .chain(zeros)
        .chain(equalities)
        .reduce(Expr::or)
        .expect("at least one item"))
}

/// Whether `value` is a float zero, also a dictionary or run-end encoded
/// one.
fn is_float_zero(value: &ScalarValue) -> bool {
    use ScalarValue as S;
    match value {
        S::Float16(Some(v)) => f32::from(*v) == 0.0,
        S::Float32(Some(v)) => *v == 0.0,
        S::Float64(Some(v)) => *v == 0.0,
        S::Dictionary(_, value) | S::RunEndEncoded(_, _, value) => is_float_zero(value),
        _ => false,
    }
}

/// Where `literal` falls among the values of `operand`'s type, when that
/// type is of an exact domain ([`exact_domain`]: an integer, a decimal of
/// any width, a date or timestamp); `Unjudged` leaves the comparison to
/// DataFusion, which happens for a float or string operand and for a
/// literal of a kind the facts do not judge (a string, a Boolean).
///
/// DataFusion would compare the two at a common type. For these operands
/// that can cast the operand, which the simplifier moves onto the literal:
/// the cast may not hold every operand value (the 38- and 76-digit decimal
/// clamps, the nanosecond range, the Float64 an Int64 meets a float at,
/// the Int64 a Decimal32 meets an Int64 at), it truncates a finer timestamp
/// (#200) and it panics on a negative-scale decimal
/// (apache/datafusion#24896). Or it can round the literal (a float at
/// `decimal(30, 15)`, #240), or there is no common type (a Decimal256
/// operand and a literal with more digits than 76 holds alongside it, a
/// pair whose common precision overflows, see `resolve`). Placing the
/// literal at the operand's own type is exact in every one of these cases,
/// so this gate never asks what the cast would lose: the operand keeps its
/// type and the comparison is decided on the literal's exact value, a
/// float's being the binary value its bits encode (decision D-j on #225:
/// `r.x == 2.0**53` on an Int64 column matches 2^53 alone). NaN and the
/// infinities are refused before this by [`interpret`].
fn placement(operand: &Expr, literal: &ScalarValue, rx: &Resolver<'_>) -> Holding {
    match rx.value_type(operand) {
        Ok(operand_type) if exact_domain(&operand_type) => held(&operand_type, literal),
        _ => Holding::Unjudged,
    }
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
        // Nothing is ordered against NaN; only `!=` holds. `interpret`
        // refuses NaN next to an exact domain, so this is the fact's
        // meaning should a caller place one.
        Placement::NotANumber => verdict_unless_null(operand, op == Operator::NotEq),
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
/// Each literal is first read next to it (`interpret`). A literal that
/// [`fit`] does not judge next to the context (a string or a Boolean, a
/// number next to a string column) is left as written for DataFusion to
/// read, as before #225 (decision D-h), and takes no part in the rule. For
/// the others:
///
/// 1. DataFusion unifies the values with the literals as they are. That is
///    kept when it is exact: no literal is inexact at the result type
///    (`fit`) and the result type holds every value of the context type
///    ([`widening`]), so the literal can widen a decimal to hold its
///    digits, or an `int32` column to the `float64` that holds every
///    `int32`. Each literal then takes its value at the result type, so
///    Arrow never casts a float at execution (at scale 33 its cast turns
///    2.5 into 2.50000000000000015216…). A float result type for a context
///    it does not hold (an `int64`, a decimal with a scale) is no
///    exception (decision D-i on #225): `r.i64.fill_null(0.0)` stays
///    Int64, and `r.i64.fill_null(1.5)` is an error. Nor is a float result
///    type that holds the context but not a literal: `coalesce(r.i32,
///    2**53 + 1, 1.5)` is an error, not the rounded `2**53`. Only a float
///    context reads a number as the nearest float (`fit`).
/// 2. Otherwise each literal the context type holds exactly takes that
///    type, and DataFusion unifies again. A literal that does not reach
///    the result type, or a result type that loses values of the context
///    type, is now a planning error naming the literal. A timestamp unit
///    widened for a literal the context does not hold is kept (decision
///    D-m: a finer literal widens the unit, as DataFusion does, and only
///    then); a nanosecond timestamp for a date context is refused, since it
///    cannot hold every date.
///
/// When DataFusion has no common type in 1, numbers next to a numeric
/// context still go on to 2: that happens only for a Decimal32, Decimal64
/// or Decimal256 context and a decimal literal with more digits than that
/// width holds alongside it, or for a pair whose common precision DataFusion
/// overflows computing (a negative-scale context and a fine-scale literal,
/// see `resolve`). There a literal the context holds exactly is as good a
/// value as anywhere else. Other kinds without a common type (a Boolean and
/// a number) are DataFusion's error.
///
/// Losses that the context itself has against the other values (two
/// columns of different types) are DataFusion's, and are left as they are.
/// So is the reading of values that are all literals, which have no
/// context: `if_else(c, 2**53 + 1, 1.5)` is the double DataFusion folds
/// them to (decision D-f), as `coalesce(2**53 + 1, 1.5)` always was.
fn values(
    mut values: Vec<Expr>,
    unify: impl Fn(Vec<Expr>) -> Result<Expr, String>,
    rx: &Resolver<'_>,
) -> Result<Vec<Expr>, String> {
    let mut literals: Vec<Option<ScalarValue>> =
        values.iter().map(|value| rx.literal(value)).collect();
    let others: Vec<&Expr> = values
        .iter()
        .zip(&literals)
        .filter_map(|(value, literal)| literal.is_none().then_some(value))
        .collect();
    if others.is_empty() || others.len() == values.len() {
        return Ok(values);
    }
    let unified_type = |values: Vec<Expr>| unify(values).and_then(|e| rx.value_type(&e));
    let nulled = values
        .iter()
        .zip(&literals)
        .map(|(value, literal)| match literal {
            Some(_) => lit(ScalarValue::Null),
            None => value.clone(),
        })
        .collect();
    let Ok(context) = unified_type(nulled) else {
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
    // Read each literal next to the context. One that is not judged there
    // becomes a value as written, like the values that are not literals.
    for (value, literal) in values.iter_mut().zip(literals.iter_mut()) {
        let Some(written) = literal.take() else {
            continue;
        };
        let read =
            replacement(interpret(&written, &context, Position::Value, &name)?).unwrap_or(written);
        if matches!(fit(&read, &context, &context), Fit::Unjudged) {
            *value = lit(read);
        } else {
            *literal = Some(read);
        }
    }
    if literals.iter().all(Option::is_none) {
        return Ok(values);
    }
    let with = |literals: &[Option<ScalarValue>]| -> Vec<Expr> {
        values
            .iter()
            .zip(literals)
            .map(|(value, literal)| literal.clone().map_or_else(|| value.clone(), lit))
            .collect()
    };
    // 1. DataFusion's unification, when it is exact.
    let exact = |literals: &[Option<ScalarValue>], result: &DataType| {
        widening(&context, result) == Widening::Exact
            && literals
                .iter()
                .flatten()
                .all(|literal| !matches!(fit(literal, result, &context), Fit::Inexact))
    };
    let numbers = context.is_numeric()
        && literals
            .iter()
            .flatten()
            .all(|literal| literal.data_type().is_numeric());
    match unified_type(with(&literals)) {
        Ok(result) if exact(&literals, &result) => {
            for literal in literals.iter_mut().flatten() {
                if let Fit::Exactly(typed) = fit(literal, &result, &context) {
                    *literal = typed;
                }
            }
        }
        Err(_) if !numbers => return Ok(values),
        _ => {
            // 2. Literals the context holds exactly take its type.
            for literal in literals.iter_mut().flatten() {
                if let Fit::Exactly(typed) = fit(literal, &context, &context) {
                    *literal = typed;
                }
            }
            let not_context = || {
                literals
                    .iter()
                    .flatten()
                    .find(|literal| literal.data_type() != context)
            };
            let refused = match unified_type(with(&literals)) {
                Ok(result) => literals
                    .iter()
                    .flatten()
                    .find(|literal| {
                        literal.data_type() != context
                            && matches!(fit(literal, &result, &context), Fit::Inexact)
                    })
                    .or_else(|| match widening(&context, &result) {
                        Widening::Exact | Widening::Finer => None,
                        Widening::Lossy => not_context(),
                    }),
                // Still no common type: a literal the context does not hold.
                Err(_) => not_context(),
            };
            if let Some(literal) = refused {
                return Err(refusal(literal, &context, &name));
            }
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
            match timestamp_literal(literal) {
                // A date is read at UTC midnight, as in comparisons.
                Some((_, _, Some(_))) => format!(
                    "{name} is a date; the timezone-aware literal {text} is not a midnight in \
                     UTC; use a date"
                ),
                _ => format!(
                    "{name} is a date; the datetime literal {text} has a time of day; use a date"
                ),
            }
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
    // Built as the SQL planner builds a CASE, not through `CaseBuilder`,
    // which refuses two literal branches of different types (`Decimal("1.5")`
    // and `Decimal("2.25")`, `1` and `2.5`) before DataFusion's coercion,
    // the one type authority (D-a), gets to unify them (D-f).
    let build = |cond: Expr, branches: Vec<Expr>| -> Result<Expr, String> {
        let [true_expr, false_expr]: [Expr; 2] = branches
            .try_into()
            .map_err(|_| "if_else has two branches".to_string())?;
        Ok(Expr::Case(Case::new(
            Some(Box::new(cond)),
            vec![(Box::new(lit(true)), Box::new(true_expr))],
            Some(Box::new(false_expr)),
        )))
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

/// A literal placed into a fixed target, a value of type `target` (never an
/// encoding) whose type does not change for it: a `shift` default, or a
/// value `insert` or `update` writes into a column. The literal is read
/// next to the target ([`interpret`]) and fitted to the target type itself
/// ([`fit`]), so the target is never widened, not even to a finer
/// timestamp unit (decisions D-c and D-m). Returns the literal as read and
/// how it fits.
fn fixed_target(
    literal: ScalarValue,
    target: &DataType,
    name: &str,
) -> Result<(ScalarValue, Fit), String> {
    let read = replacement(interpret(&literal, target, Position::Value, name)?).unwrap_or(literal);
    let fit = fit(&read, target, target);
    Ok((read, fit))
}

/// A `shift` default, read next to the shifted values. `lag`/`lead` cast it
/// to the column's type, so it must be a value of that type
/// ([`fixed_target`]): a default the column cannot hold exactly is an
/// error rather than a truncated, rounded or overflowing value (decision
/// D-c). A default `fit` does not judge (a string, a Boolean, a number for
/// a string column) is left as written for that cast, as before #225 (D-h).
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
    match fixed_target(default, &column_type, &name)? {
        (_, Fit::Exactly(value)) => Ok(value),
        (read, Fit::Unjudged) => Ok(read),
        (read, Fit::Inexact) => Err(format!(
            "{name} cannot hold the shift() default {} exactly ({column_type})",
            literal_text(&read)
        )),
    }
}

/// The value `insert` or `update` writes into a column of `column_type`
/// (`name` describes the column in errors), as a scalar of exactly that
/// type. The column is a fixed target ([`fixed_target`]), as a `shift`
/// default's is: its type never changes, and a literal it cannot hold
/// exactly is an error naming the literal. A dictionary or run-end-encoded
/// column takes a value of its value type, in the column's encoding.
///
/// A shift default that `fit` does not judge is cast by `lag`/`lead` at
/// execution. A written value has no such cast after it, so the pairs `fit`
/// leaves unjudged are decided here: a string is a value of every string
/// type, and every other such pair (a number for a string column, a string
/// or Boolean for a number or date column, any literal for a binary, list
/// or duration column) would change the value's kind and is an error.
pub(crate) fn column_value(
    literal: ScalarValue,
    column_type: &DataType,
    name: &str,
) -> Result<ScalarValue, String> {
    match column_type {
        DataType::Dictionary(key, values) => {
            let value = column_value(literal, values, name)?;
            return Ok(ScalarValue::Dictionary(key.clone(), Box::new(value)));
        }
        DataType::RunEndEncoded(run_ends, values) => {
            let value = column_value(literal, values.data_type(), name)?;
            return Ok(ScalarValue::RunEndEncoded(
                Arc::clone(run_ends),
                Arc::clone(values),
                Box::new(value),
            ));
        }
        _ => {}
    }
    let value = if literal.is_null() {
        ScalarValue::try_new_null(column_type).ok()
    } else {
        match fixed_target(literal, column_type, name)? {
            (_, Fit::Exactly(value)) => Some(value),
            (read, Fit::Inexact) => {
                return Err(format!(
                    "{name} cannot hold {} exactly ({column_type})",
                    literal_text(&read)
                ))
            }
            (read, Fit::Unjudged) if is_string(&read.data_type()) && is_string(column_type) => {
                read.cast_to(column_type).ok()
            }
            (read, Fit::Unjudged) => {
                // Written as Python writes these values.
                let text = match &read {
                    ScalarValue::Boolean(Some(true)) => "True".to_string(),
                    ScalarValue::Boolean(Some(false)) => "False".to_string(),
                    ScalarValue::Utf8(Some(s)) => format!("'{s}'"),
                    other => literal_text(other),
                };
                return Err(format!(
                    "{name} ({column_type}) cannot hold the {} value {text}; write a value of \
                     the column's type",
                    read.data_type()
                ));
            }
        }
    };
    // `fit` gives a value of the type it was asked about; a typed NULL
    // exists for every type but a few nested ones.
    value
        .filter(|value| value.data_type() == *column_type)
        .ok_or_else(|| {
            format!("{name} has type {column_type}, which insert and update cannot write")
        })
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// The other operand of `dt.diff`, read next to the receiver like an
/// arithmetic operand (decision D-d). A number or Boolean is left as it
/// is: `dt.diff` refuses it itself, in its own words.
pub(crate) fn dt_diff_other(on: &Expr, other: Expr, rx: &Resolver<'_>) -> Result<Expr, String> {
    match rx.literal(&other) {
        Some(literal)
            if literal.data_type().is_numeric() || matches!(literal, ScalarValue::Boolean(_)) =>
        {
            Ok(other)
        }
        Some(literal) => {
            Ok(replacement(read(&literal, on, Position::Arithmetic, rx)?).map_or(other, lit))
        }
        None => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use DataType as T;
    use ScalarValue as S;

    fn write(literal: S, column_type: &T) -> Result<S, String> {
        column_value(literal, column_type, "column 'c'")
    }

    #[test]
    fn a_written_value_has_the_column_type() {
        let dictionary = T::Dictionary(Box::new(T::Int8), Box::new(T::LargeUtf8));
        let table: Vec<(S, T, S)> = vec![
            (S::Int64(Some(3)), T::UInt8, S::UInt8(Some(3))),
            (S::Float64(Some(2.0)), T::Int32, S::Int32(Some(2))),
            (
                S::Decimal128(Some(150), 3, 2),
                T::Decimal128(5, 1),
                S::Decimal128(Some(15), 5, 1),
            ),
            // A float column reads a number as the nearest float (D-i).
            (S::Float64(Some(0.1)), T::Float32, S::Float32(Some(0.1))),
            (
                S::Utf8(Some("a".into())),
                T::Utf8View,
                S::Utf8View(Some("a".into())),
            ),
            (
                S::Utf8(Some("a".into())),
                dictionary.clone(),
                S::Dictionary(Box::new(T::Int8), Box::new(S::LargeUtf8(Some("a".into())))),
            ),
            (
                S::Null,
                dictionary,
                S::Dictionary(Box::new(T::Int8), Box::new(S::LargeUtf8(None))),
            ),
            (
                S::Null,
                T::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
                S::TimestampSecond(None, Some(Arc::from("UTC"))),
            ),
            (S::Date32(Some(1)), T::Date64, S::Date64(Some(86_400_000))),
        ];
        for (literal, column_type, expected) in table {
            assert_eq!(
                write(literal.clone(), &column_type),
                Ok(expected),
                "{literal:?} into {column_type}"
            );
        }
    }

    #[test]
    fn a_value_the_column_cannot_hold_exactly_is_refused() {
        let table: Vec<(S, T, &str)> = vec![
            (
                S::Int64(Some(300)),
                T::Int8,
                "column 'c' cannot hold 300 exactly (Int8)",
            ),
            (
                S::Int64(Some(-1)),
                T::UInt64,
                "column 'c' cannot hold -1 exactly (UInt64)",
            ),
            (
                S::Float64(Some(1.5)),
                T::Int64,
                "column 'c' cannot hold 1.5 exactly (Int64)",
            ),
            (
                S::Decimal128(Some(1234), 4, 3),
                T::Decimal128(5, 2),
                "column 'c' cannot hold 1.234 exactly (Decimal128(5, 2))",
            ),
            // A float column takes the nearest float within its range only.
            (
                S::Float64(Some(1e39)),
                T::Float32,
                "column 'c' cannot hold 1000000000000000000000000000000000000000 exactly (Float32)",
            ),
            // A finer timestamp never widens the column (D-m).
            (
                S::TimestampNanosecond(Some(1_000_000_001), None),
                T::Timestamp(TimeUnit::Microsecond, None),
                "column 'c' cannot hold 1970-01-01 00:00:01.000000001 exactly (Timestamp(µs))",
            ),
        ];
        for (literal, column_type, message) in table {
            assert_eq!(write(literal, &column_type), Err(message.to_string()));
        }
    }

    #[test]
    fn a_value_of_another_kind_is_refused_not_cast() {
        let table: Vec<(S, T, &str)> = vec![
            (S::Int64(Some(5)), T::Utf8, "the Int64 value 5"),
            (S::Utf8(Some("1".into())), T::Int64, "the Utf8 value '1'"),
            (S::Boolean(Some(true)), T::Int64, "the Boolean value True"),
            (S::Int64(Some(1)), T::Boolean, "the Int64 value 1"),
            (
                S::Utf8(Some("2024-01-01".into())),
                T::Date32,
                "the Utf8 value '2024-01-01'",
            ),
            (
                S::Int64(Some(1)),
                T::Duration(TimeUnit::Second),
                "the Int64 value 1",
            ),
        ];
        for (literal, column_type, value) in table {
            let message = write(literal, &column_type).unwrap_err();
            assert_eq!(
                message,
                format!("column 'c' ({column_type}) cannot hold {value}; write a value of the column's type")
            );
        }
    }

    #[test]
    fn an_encoded_column_takes_a_value_of_its_value_type() {
        let run_ends = Arc::new(Field::new("run_ends", T::Int32, false));
        let values = Arc::new(Field::new("values", T::Int64, true));
        let column_type = T::RunEndEncoded(Arc::clone(&run_ends), Arc::clone(&values));
        let encoded =
            |value| S::RunEndEncoded(Arc::clone(&run_ends), Arc::clone(&values), Box::new(value));
        assert_eq!(
            write(S::Float64(Some(2.0)), &column_type),
            Ok(encoded(S::Int64(Some(2))))
        );
        assert_eq!(write(S::Null, &column_type), Ok(encoded(S::Int64(None))));
        assert_eq!(
            write(S::Float64(Some(2.5)), &column_type),
            Err("column 'c' cannot hold 2.5 exactly (Int64)".to_string())
        );
    }
}
