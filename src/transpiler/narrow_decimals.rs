//! `decimal32` and `decimal64` values widen to `decimal128` (contract §17.4
//! rule 2, #241).
//!
//! DataFusion 55 gives these two widths a common type with an integer only
//! when the integer is narrow enough for them (up to Int16 for `decimal32`,
//! up to Int32 for `decimal64`, `coerce_numeric_type_to_decimal32/64`). Next
//! to a wider integer, which every Python `int` literal and most integer
//! columns are, the pair falls through to integer coercion, and the decimal
//! values are cast to the integer type: `1.50 > 1` is false. Two narrow
//! decimals meet at a narrow decimal whose precision is clamped to the
//! width's maximum, so `decimal32(9, 2)` and `decimal32(9, 3)` meet at
//! `decimal32(9, 3)`, which does not hold 9999999.99, and their sum is a
//! `decimal32` that does not hold 9999999.99 + 0.01.
//!
//! So a narrow decimal is cast to the `decimal128` of the same precision and
//! scale, which holds every one of its values, before DataFusion coerces the
//! node it is an operand of:
//!
//! - always, as an operand of `+`, `-`, `*`, `/` and `%`, and as the argument
//!   of `sum` and `avg` (an aggregate or a window), so that their result
//!   types only ever come from `decimal128` and `decimal256` operands;
//! - where it meets a value of another integer or decimal type: the operands
//!   of a comparison, `IN` and `BETWEEN`, and the values that share a result
//!   column (`coalesce`, the branches of a CASE).
//!
//! A literal is not a value it meets: a literal among shared values takes
//! the type of the others and never widens it (§17.5 rule 1), and a literal
//! in a comparison is placed among the operand's own values
//! (`literals::placement`), at the operand's type. A float among the values
//! leaves them all to DataFusion, which computes a narrow decimal next to a
//! float in `float64`, the type §17.4 rule 1 gives shared values, where a
//! `decimal128` would meet the float at `decimal(30, 15)` and fail on NaN,
//! infinity and values past 10^15 (#228, which owns the float pairing).
//! Arithmetic with a float is `float64` either way. A narrow decimal that meets
//! only values of its own type keeps its type, so an untouched `decimal32`
//! column stays `decimal32`.
//!
//! [`widen`] is one node's rewrite; `resolve::Resolver` applies it to every
//! node before DataFusion's coercion of that node, and answers the type of
//! each operand, as it answers every type question. A join key pair and a
//! union column are not expressions, and their plan builders ask [`widens`]
//! and [`widened`] directly.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::{
    AggregateFunction, Between, Case, InList, ScalarFunction, WindowFunctionDefinition,
};
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, Operator};

/// The type an operand executes as, or `None` when DataFusion cannot tell.
pub(crate) type TypeOf<'t> = dyn Fn(&Expr) -> Option<DataType> + 't;

/// The `decimal128` a `decimal32` or `decimal64` type widens to, or `None`
/// for any other type. An encoded column widens by its value type: the cast
/// decodes it, as DataFusion's coercion of an encoded operand does.
pub(crate) fn wide_type(data_type: &DataType) -> Option<DataType> {
    match values_of(data_type) {
        DataType::Decimal32(precision, scale) | DataType::Decimal64(precision, scale) => {
            Some(DataType::Decimal128(*precision, *scale))
        }
        _ => None,
    }
}

/// `expr`, a value of `data_type`, cast to the `decimal128` it widens to
/// when it is a narrow decimal ([`wide_type`]).
pub(crate) fn widened(expr: Expr, data_type: &DataType) -> Expr {
    match wide_type(data_type) {
        Some(wide) => Expr::Cast(Cast::new(Box::new(expr), wide)),
        None => expr,
    }
}

/// Whether values of `types` that meet at one common type widen their
/// narrow decimals: one is a narrow decimal, another is an integer or a
/// decimal of a different type, and none is of another kind. NULL takes no
/// part.
pub(crate) fn widens<'t>(types: impl IntoIterator<Item = &'t DataType>) -> bool {
    let types: Vec<&DataType> = types
        .into_iter()
        .map(values_of)
        .filter(|t| **t != DataType::Null)
        .collect();
    types.iter().any(|t| wide_type(t).is_some())
        && types.iter().all(|t| t.is_integer() || t.is_decimal())
        && types.iter().any(|t| t != &types[0])
}

/// The type of the values of `data_type`, without dictionary or run-end
/// encoding (as `Resolver::value_type`).
fn values_of(mut data_type: &DataType) -> &DataType {
    loop {
        data_type = match data_type {
            DataType::Dictionary(_, values) => values,
            DataType::RunEndEncoded(_, values) => values.data_type(),
            decoded => return decoded,
        };
    }
}

/// `expr` with the narrow decimals among its operands widened, for one node
/// whose operands DataFusion has already coerced, `type_of` giving their
/// types. Nodes of other kinds are returned as they are, and so is a node
/// with an operand whose type DataFusion cannot tell: DataFusion reports
/// that problem itself.
pub(crate) fn widen(expr: Expr, type_of: &TypeOf<'_>) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let operands = vec![*left, *right];
            let widened = if op.is_numerical_operators() {
                every_narrow(operands, type_of)
            } else if is_comparison(op) {
                meeting(operands, type_of)
            } else {
                Transformed::no(operands)
            };
            rebuild(widened, |operands| {
                let [left, right]: [Expr; 2] = operands.try_into().expect("two operands");
                Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
            })
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => rebuild(meeting(vec![*expr, *low, *high], type_of), |values| {
            let [expr, low, high]: [Expr; 3] = values.try_into().expect("three values");
            Expr::Between(Between::new(
                Box::new(expr),
                negated,
                Box::new(low),
                Box::new(high),
            ))
        }),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let values = std::iter::once(*expr).chain(list).collect();
            rebuild(meeting(values, type_of), |mut values| {
                let expr = values.remove(0);
                Expr::InList(InList::new(Box::new(expr), values, negated))
            })
        }
        Expr::Case(case) => widen_case(case, type_of),
        Expr::ScalarFunction(ScalarFunction { func, args }) if func.name() == "coalesce" => {
            rebuild(meeting(args, type_of), |args| {
                Expr::ScalarFunction(ScalarFunction { func, args })
            })
        }
        Expr::AggregateFunction(AggregateFunction { func, mut params })
            if is_sum_or_mean(func.name()) =>
        {
            let args = every_narrow(std::mem::take(&mut params.args), type_of);
            rebuild(args, |args| {
                params.args = args;
                Expr::AggregateFunction(AggregateFunction { func, params })
            })
        }
        Expr::WindowFunction(mut window) if matches!(&window.fun, WindowFunctionDefinition::AggregateUDF(f) if is_sum_or_mean(f.name())) =>
        {
            let args = every_narrow(std::mem::take(&mut window.params.args), type_of);
            rebuild(args, |args| {
                window.params.args = args;
                Expr::WindowFunction(window)
            })
        }
        other => Transformed::no(other),
    }
}

/// A CASE: its operand and WHEN values are compared with one another, and
/// its THEN and ELSE values share the result column.
fn widen_case(case: Case, type_of: &TypeOf<'_>) -> Transformed<Expr> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let (whens, thens): (Vec<Expr>, Vec<Expr>) =
        when_then_expr.into_iter().map(|(w, t)| (*w, *t)).unzip();
    let (operand, whens, compared) = match expr {
        Some(operand) => {
            let compared = meeting(std::iter::once(*operand).chain(whens).collect(), type_of);
            let mut values = compared.data;
            let operand = values.remove(0);
            (Some(Box::new(operand)), values, compared.transformed)
        }
        None => (None, whens, false),
    };
    let branches = thens.len();
    let shared = meeting(
        thens.into_iter().chain(else_expr.map(|e| *e)).collect(),
        type_of,
    );
    let transformed = compared || shared.transformed;
    let mut values = shared.data;
    let else_expr = (values.len() > branches).then(|| Box::new(values.pop().expect("an ELSE")));
    let when_then_expr = whens
        .into_iter()
        .zip(values)
        .map(|(w, t)| (Box::new(w), Box::new(t)))
        .collect();
    Transformed::new_transformed(
        Expr::Case(Case::new(operand, when_then_expr, else_expr)),
        transformed,
    )
}

/// `values` with every narrow decimal among them widened.
fn every_narrow(values: Vec<Expr>, type_of: &TypeOf<'_>) -> Transformed<Vec<Expr>> {
    let Some(types) = types_of(&values, type_of) else {
        return Transformed::no(values);
    };
    let transformed = types.iter().any(|t| wide_type(t).is_some());
    let values = values
        .into_iter()
        .zip(&types)
        .map(|(value, t)| widened(value, t))
        .collect();
    Transformed::new_transformed(values, transformed)
}

/// `values`, which meet at one common type, with their narrow decimals
/// widened when one meets a value of another integer or decimal type
/// ([`widens`]). Literals take no part, and are never cast.
fn meeting(values: Vec<Expr>, type_of: &TypeOf<'_>) -> Transformed<Vec<Expr>> {
    let Some(types) = types_of(values.iter().filter(|v| !is_literal(v)), type_of) else {
        return Transformed::no(values);
    };
    if !widens(&types) {
        return Transformed::no(values);
    }
    let mut types = types.iter();
    let values = values
        .into_iter()
        .map(|value| match is_literal(&value) {
            true => value,
            false => widened(value, types.next().expect("a type per value")),
        })
        .collect();
    Transformed::yes(values)
}

/// The node `build` makes of `operands`, transformed when they were.
fn rebuild(
    operands: Transformed<Vec<Expr>>,
    build: impl FnOnce(Vec<Expr>) -> Expr,
) -> Transformed<Expr> {
    Transformed::new_transformed(build(operands.data), operands.transformed)
}

/// The type of each of `values`, or `None` when DataFusion cannot tell one.
fn types_of<'e>(
    values: impl IntoIterator<Item = &'e Expr>,
    type_of: &TypeOf<'_>,
) -> Option<Vec<DataType>> {
    values.into_iter().map(type_of).collect()
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(..))
}

fn is_comparison(op: Operator) -> bool {
    use Operator::*;
    matches!(
        op,
        Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom
    )
}

/// `sum` and `avg`, whose decimal result types are formulas of the input's
/// precision and scale (§14.2). `min`, `max` and the other aggregates keep
/// or replace the input type and need no widening.
fn is_sum_or_mean(name: &str) -> bool {
    matches!(name, "sum" | "avg")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
    use datafusion::common::DFSchema;
    use datafusion::functions::core::expr_fn::coalesce;
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::logical_expr::{case, col, lit};
    use std::sync::Arc;
    use DataType as T;

    fn schema() -> DFSchema {
        let dictionary = T::Dictionary(Box::new(T::Int8), Box::new(T::Decimal32(9, 2)));
        DFSchema::try_from(ArrowSchema::new(vec![
            Field::new("d32", T::Decimal32(9, 2), true),
            Field::new("d32s3", T::Decimal32(9, 3), true),
            Field::new("d64", T::Decimal64(18, 2), true),
            Field::new("d128", T::Decimal128(9, 2), true),
            Field::new("dict", dictionary, true),
            Field::new("i64", T::Int64, true),
            Field::new("f64", T::Float64, true),
            Field::new("s", T::Utf8, true),
            Field::new("b", T::Boolean, true),
        ]))
        .unwrap()
    }

    fn wide(name: &str, precision: u8, scale: i8) -> Expr {
        Expr::Cast(Cast::new(
            Box::new(col(name)),
            T::Decimal128(precision, scale),
        ))
    }

    /// The type of a column, a literal or a cast; the operands these tests
    /// build are nothing else.
    fn type_of(expr: &Expr) -> Option<DataType> {
        match expr {
            Expr::Column(c) => schema()
                .field_with_unqualified_name(&c.name)
                .ok()
                .map(|f| f.data_type().clone()),
            Expr::Literal(value, _) => Some(value.data_type()),
            Expr::Cast(cast) => Some(cast.field.data_type().clone()),
            _ => None,
        }
    }

    fn check(expr: Expr, expected: Expr) {
        let widened = widen(expr.clone(), &type_of);
        assert_eq!(widened.data, expected, "{expr}");
        assert_eq!(widened.transformed, expected != expr, "{expr}");
    }

    #[test]
    fn a_narrow_decimal_widens_where_it_meets_another_integer_or_decimal() {
        check(col("d32").gt(col("i64")), wide("d32", 9, 2).gt(col("i64")));
        check(col("i64").eq(col("d64")), col("i64").eq(wide("d64", 18, 2)));
        // Of another scale, and of another width.
        check(
            col("d32").lt(col("d32s3")),
            wide("d32", 9, 2).lt(wide("d32s3", 9, 3)),
        );
        check(
            col("d32").eq(col("d128")),
            wide("d32", 9, 2).eq(col("d128")),
        );
        check(
            col("dict").eq(col("i64")),
            wide("dict", 9, 2).eq(col("i64")),
        );
        check(
            col("d32").in_list(vec![col("i64"), lit(1i64)], false),
            wide("d32", 9, 2).in_list(vec![col("i64"), lit(1i64)], false),
        );
        check(
            col("d32").between(col("i64"), col("d64")),
            wide("d32", 9, 2).between(col("i64"), wide("d64", 18, 2)),
        );
        check(
            coalesce(vec![col("d32"), col("i64")]),
            coalesce(vec![wide("d32", 9, 2), col("i64")]),
        );
        check(
            case(col("b"))
                .when(lit(true), col("i64"))
                .otherwise(col("d32"))
                .unwrap(),
            case(col("b"))
                .when(lit(true), col("i64"))
                .otherwise(wide("d32", 9, 2))
                .unwrap(),
        );
        check(
            case(col("d32")).when(col("i64"), lit(1i64)).end().unwrap(),
            case(wide("d32", 9, 2))
                .when(col("i64"), lit(1i64))
                .end()
                .unwrap(),
        );
    }

    #[test]
    fn a_narrow_decimal_keeps_its_type_next_to_its_own_type_a_literal_a_float_or_null() {
        for expr in [
            col("d32").eq(col("d32")),
            col("d32").eq(col("dict")),
            col("d32").gt(lit(1i64)),
            coalesce(vec![col("d32"), lit(1i64)]),
            coalesce(vec![col("d32"), lit(datafusion::scalar::ScalarValue::Null)]),
            case(col("b")).when(lit(true), col("d32")).end().unwrap(),
            col("d32").gt(col("f64")),
            coalesce(vec![col("d32"), col("i64"), col("f64")]),
            col("d32").eq(col("s")),
            max(col("d32")),
        ] {
            check(expr.clone(), expr);
        }
    }

    #[test]
    fn arithmetic_and_sums_always_widen() {
        check(
            col("d32") + col("d32"),
            wide("d32", 9, 2) + wide("d32", 9, 2),
        );
        check(col("d64") * lit(2i64), wide("d64", 18, 2) * lit(2i64));
        check(col("d32") - col("f64"), wide("d32", 9, 2) - col("f64"));
        check(col("d128") + col("i64"), col("d128") + col("i64"));
        check(sum(col("d32")), sum(wide("d32", 9, 2)));
        check(sum(col("d128")), sum(col("d128")));
    }

    #[test]
    fn a_node_datafusion_cannot_type_is_left_alone() {
        let expr = col("missing").gt(col("d32"));
        check(expr.clone(), expr);
    }

    #[test]
    fn widening_types() {
        let i = T::Int64;
        let d32 = T::Decimal32(9, 2);
        let d128 = T::Decimal128(9, 2);
        assert!(widens([&d32, &i]));
        assert!(!widens([&d32, &d32]));
        assert!(!widens([&d32, &T::Null]));
        assert!(!widens([&d32, &i, &T::Float32]));
        assert!(!widens([&d128, &i]));
        let run_end = T::RunEndEncoded(
            Arc::new(Field::new("run_ends", T::Int32, false)),
            Arc::new(Field::new("values", T::Decimal64(18, 2), true)),
        );
        assert_eq!(wide_type(&run_end), Some(T::Decimal128(18, 2)));
        assert!(widens([&run_end, &i]));
    }
}
