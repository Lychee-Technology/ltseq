//! How ltseq reads a literal next to a value of a known type.
//!
//! DataFusion coerces a literal the way it coerces a column. For a few
//! literal kinds that gives an answer ltseq decided against (#145, and the
//! design review on #225). [`interpret`] is the complete list of those
//! exceptions:
//!
//! | literal | next to | read as | where |
//! |---|---|---|---|
//! | Decimal | Float32, Float64 | the nearest float of that width (design §2.6) | everywhere |
//! | Decimal, date, datetime | a string | an error (design §2.6, plan change 4) | comparisons, values |
//! | date, datetime | a number | an error (pre-PR review I-3) | comparisons, values |
//! | number, Boolean | a date or timestamp | an error (a cast reads `5` as 1970-01-06) | values |
//! | naive datetime | a zoned timestamp | wall-clock time in that zone; a time the zone skips or repeats is an error (design §2.6) | everywhere |
//! | date | a zoned timestamp | midnight in that zone, at the timestamp's unit (§5 row 5, P10) | everywhere |
//! | aware datetime | a naive timestamp | an error (D5) | everywhere |
//!
//! "Everywhere" includes arithmetic and `dt.diff` (decision D-d: one literal
//! has one meaning). The context type comes from `Resolver::value_type`, so
//! it is never an encoding; nothing here looks at an expression or a schema.
//!
//! One more reading is not an exception in [`interpret`] but a consequence
//! of [`held`]: an aware datetime next to a zoned timestamp of another zone
//! is the same instant in the column's zone, everywhere. The facts call a
//! change of zone a change of kind (the review on #225: tz A ↔ tz B is
//! `Kind` for the facts, the policy decides), so a shared result column
//! never takes the literal's zone; the literal takes the column's.
//!
//! The rest of the module is the policy over the facts in `exact`: which
//! operand types ltseq decides comparisons for ([`exact_domain`]), whether a
//! type holds a literal ([`held`]), whether a literal fits the type of the
//! values it shares a result column with ([`fit`], decision D-b) and what a
//! wider result type does to those values ([`widening`], D-m). The
//! decisions on #225 this module applies, in one place:
//!
//! - D-b/D-i: a literal that shares a result column takes the type of the
//!   other values when that type holds it exactly; a float literal is no
//!   exception ([`fit`]).
//! - D-j: a float literal is the binary value its bits encode. NaN and the
//!   infinities are values of no exact domain, so next to an integer,
//!   decimal, date or timestamp they are refused at planning
//!   ([`interpret`]); a float column keeps DataFusion's float semantics.
//! - D-k (D-h kept): strings and Booleans, and a number next to a string
//!   type, are DataFusion's to read (`Unjudged`, never exact).
//! - D-l: a number or Boolean next to a date or timestamp is refused in
//!   every position; `dt.add` is the way to add a duration.
//! - D-m: a shared result column may widen a timestamp to a finer unit,
//!   on range alone, when a literal needs it ([`widening`]); nothing else
//!   widens, and a `shift` default never does.

use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;

use super::exact::{
    cast_class, exact_ticks, hold_instant, holds, ticks_per_second, CastClass, Holding,
};
use crate::types::{decimal_text, decimal_to_f64, timestamp_scalar};

/// How ltseq reads a literal next to a value of some type.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Reading {
    /// DataFusion reads the literal as ltseq does: keep it.
    Keep,
    /// Use this literal instead.
    Value(ScalarValue),
    /// The instant a naive or date literal means in a zone, as ticks of
    /// `unit`, when it is exact in no unit whose `i64` range holds it. Only
    /// a comparison can use it, by placing it among the operand's values; in
    /// any other position such a literal is an error.
    Instant(i128, TimeUnit),
}

/// Where a literal meets the value it is read against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Position {
    /// A comparison operand or an `is_in` item.
    Comparison,
    /// An arithmetic operand, or the other operand of `dt.diff`.
    Arithmetic,
    /// A value that shares a result column with others: a branch of
    /// `if_else`, an argument of `coalesce`/`fill_null`, a `shift` default.
    Value,
}

/// How to read `literal` next to a value of type `context` in `position`,
/// or an error. `name` describes the value in errors ("column 'x'").
pub(crate) fn interpret(
    literal: &ScalarValue,
    context: &DataType,
    position: Position,
    name: &str,
) -> Result<Reading, String> {
    use DataType as T;
    use ScalarValue as S;
    if literal.is_null() {
        return Ok(Reading::Keep);
    }
    let timestamp = timestamp_literal(literal);
    let temporal = timestamp.is_some() || matches!(literal, S::Date32(_));
    let kinds_checked = position != Position::Arithmetic;
    match (context, literal) {
        (T::Float64, S::Decimal128(Some(value), _, scale)) => Ok(Reading::Value(S::Float64(Some(
            decimal_to_f64(*value, *scale),
        )))),
        (T::Float32, S::Decimal128(Some(value), _, scale)) => {
            // Parsed from the decimal text, so it is the correctly rounded float32.
            let float = decimal_text(*value, *scale)
                .parse::<f32>()
                .map_err(|e| format!("Decimal literal as float32: {e}"))?;
            Ok(Reading::Value(S::Float32(Some(float))))
        }
        (T::Utf8 | T::LargeUtf8 | T::Utf8View, _)
            if kinds_checked && (temporal || matches!(literal, S::Decimal128(..))) =>
        {
            Err(format!(
                "{name} is a string; use a string literal, or cast it to the literal's type first"
            ))
        }
        // D-l, mirrored: a number is no count of days to add to a date.
        (t, _) if temporal && t.is_numeric() => Err(format!(
            "{name} is numeric ({context}); use a number, not a date or datetime"
        )),
        // D-l: a number next to a date or timestamp is a type error in
        // every position; it is not a count of days or seconds.
        (T::Date32 | T::Date64 | T::Timestamp(..), _)
            if literal.data_type().is_numeric() || matches!(literal, S::Boolean(_)) =>
        {
            let hint = if position == Position::Arithmetic {
                "; dt.add() adds days, months or seconds"
            } else {
                ""
            };
            Err(format!(
                "{name} is a date or timestamp ({context}); use a date or datetime, not {}{hint}",
                literal_text(literal)
            ))
        }
        // D-j: no integer or decimal equals, exceeds or falls below NaN or
        // an infinity; the extended reals are not a rule ltseq adds.
        (t, _) if kinds_checked && exact_domain(t) && !finite(literal) => Err(format!(
            "{name} is {context}, which has no value for {}; NaN and infinity meet only float \
             columns",
            literal_text(literal)
        )),
        (T::Timestamp(unit, zone), _) => zoned(literal, *unit, zone.as_deref(), position, name),
        _ => Ok(Reading::Keep),
    }
}

/// A date or timestamp literal against a timestamp of `unit` in `zone`:
/// the zone rules of the table above.
/// Whether a literal is a finite value: false only for a float NaN or infinity.
fn finite(literal: &ScalarValue) -> bool {
    match literal {
        ScalarValue::Float32(Some(v)) => v.is_finite(),
        ScalarValue::Float64(Some(v)) => v.is_finite(),
        _ => true,
    }
}

fn zoned(
    literal: &ScalarValue,
    unit: TimeUnit,
    zone: Option<&str>,
    position: Position,
    name: &str,
) -> Result<Reading, String> {
    let beyond = |zone: &str, instant: i128, instant_unit: TimeUnit| {
        if position == Position::Comparison {
            Ok(Reading::Instant(instant, instant_unit))
        } else {
            Err(format!(
                "{literal} in {zone} is outside the range of {name}"
            ))
        }
    };
    match (timestamp_literal(literal), literal, zone) {
        (Some((_, _, Some(lit_zone))), _, None) => Err(format!(
            "{name} is timezone-naive, but the literal is timezone-aware ({lit_zone}); \
             use a naive datetime"
        )),
        (Some((value, lit_unit, None)), _, Some(zone)) => {
            let instant = local_to_utc(i128::from(value), lit_unit, zone)?;
            // At the literal's own unit, keeping its precision, or at the
            // operand's when the zone offset moved the instant past the range
            // of the literal's unit.
            let exact = [lit_unit, unit].into_iter().find_map(|to| {
                exact_ticks(instant, lit_unit, to)
                    .map(|ticks| timestamp_scalar(to, Some(ticks), Some(zone.into())))
            });
            match exact {
                Some(value) => Ok(Reading::Value(value)),
                None => beyond(zone, instant, lit_unit),
            }
        }
        (None, ScalarValue::Date32(Some(days)), Some(zone)) => {
            let midnight = i128::from(*days) * i128::from(86_400 * ticks_per_second(unit));
            let instant = local_to_utc(midnight, unit, zone)?;
            match i64::try_from(instant) {
                Ok(ticks) => Ok(Reading::Value(timestamp_scalar(
                    unit,
                    Some(ticks),
                    Some(zone.into()),
                ))),
                Err(_) => beyond(zone, instant, unit),
            }
        }
        _ => Ok(Reading::Keep),
    }
}

/// A non-null timestamp literal: its ticks, unit and zone.
pub(crate) fn timestamp_literal(literal: &ScalarValue) -> Option<(i64, TimeUnit, Option<&str>)> {
    let (value, unit, zone) = match literal {
        ScalarValue::TimestampSecond(Some(v), tz) => (*v, TimeUnit::Second, tz),
        ScalarValue::TimestampMillisecond(Some(v), tz) => (*v, TimeUnit::Millisecond, tz),
        ScalarValue::TimestampMicrosecond(Some(v), tz) => (*v, TimeUnit::Microsecond, tz),
        ScalarValue::TimestampNanosecond(Some(v), tz) => (*v, TimeUnit::Nanosecond, tz),
        _ => return None,
    };
    Some((value, unit, zone.as_deref()))
}

/// The instant at which the wall-clock time `value` (ticks of `unit`) occurs
/// in `zone`, in ticks of `unit`. Either may be outside the `i64` range the
/// other fits; callers place the instant at the unit they need. A time the
/// zone skips (a DST gap) or repeats (a DST fold) is an error: no single
/// instant is meant.
pub(crate) fn local_to_utc(value: i128, unit: TimeUnit, zone: &str) -> Result<i128, String> {
    let tz: Tz = zone
        .parse()
        .map_err(|_| format!("'{zone}' is not a valid time zone"))?;
    let per_second = ticks_per_second(unit);
    let naive = naive_datetime(value, per_second)
        .ok_or_else(|| format!("timestamp literal {value} is outside the supported range"))?;
    let offset_seconds = match tz.from_local_datetime(&naive) {
        LocalResult::Single(local) => local.offset().fix().local_minus_utc(),
        LocalResult::None => {
            return Err(format!(
                "the naive datetime {naive} does not exist in {zone} (a daylight-saving gap); \
                 use a timezone-aware datetime instead"
            ))
        }
        LocalResult::Ambiguous(..) => {
            return Err(format!(
                "the naive datetime {naive} is ambiguous in {zone} (it occurs twice when \
                 daylight saving ends); use a timezone-aware datetime instead"
            ))
        }
    };
    Ok(value - i128::from(offset_seconds) * i128::from(per_second))
}

/// How errors write a literal: a Decimal by its digits, a date or a
/// timestamp as calendar text (an aware one as its local time and zone),
/// anything else as DataFusion displays it.
pub(crate) fn literal_text(literal: &ScalarValue) -> String {
    match literal {
        ScalarValue::Decimal128(Some(value), _, scale) => decimal_text(*value, *scale),
        ScalarValue::Date32(Some(days)) => naive_datetime(i128::from(*days) * 86_400, 1)
            .map_or(days.to_string(), |d| d.date().to_string()),
        _ => match timestamp_literal(literal) {
            Some((value, unit, zone)) => {
                let Some(utc) = naive_datetime(i128::from(value), ticks_per_second(unit)) else {
                    return value.to_string();
                };
                // An aware timestamp's ticks are UTC.
                match zone.map(|zone| (zone, zone.parse::<Tz>())) {
                    None => utc.to_string(),
                    Some((zone, Ok(tz))) => {
                        format!("{} {zone}", tz.from_utc_datetime(&utc).naive_local())
                    }
                    Some(_) => format!("{utc} UTC"),
                }
            }
            None => literal.to_string(),
        },
    }
}

fn naive_datetime(value: i128, per_second: i64) -> Option<NaiveDateTime> {
    let per_second = i128::from(per_second);
    let seconds = i64::try_from(value.div_euclid(per_second)).ok()?;
    let nanos = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
    DateTime::from_timestamp(seconds, u32::try_from(nanos).ok()?).map(|utc| utc.naive_utc())
}

/// The types whose comparisons ltseq decides for itself: integers, decimals
/// of any width, dates and timestamps, which hold each of their values
/// exactly. A float operand keeps DataFusion's float semantics (decision
/// D-j on #225); strings, Booleans and the other kinds keep DataFusion's
/// reading (D-h).
pub(crate) fn exact_domain(t: &DataType) -> bool {
    t.is_integer()
        || t.is_decimal()
        || matches!(
            t,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
        )
}

/// Whether `context` holds `literal`: [`holds`], plus the one zone reading
/// the facts leave to policy. The days of a date type are UTC midnights, as
/// comparisons have always read them (P10 on #145), so a zoned timestamp is
/// held by a date type at its UTC instant. (A naive literal next to a zoned
/// type is given its zone by [`interpret`] before it gets here.)
pub(crate) fn held(context: &DataType, literal: &ScalarValue) -> Holding {
    match (context, timestamp_literal(literal)) {
        (DataType::Date32 | DataType::Date64, Some((ticks, unit, Some(_)))) => {
            hold_instant(i128::from(ticks), unit, context)
        }
        _ => holds(context, literal),
    }
}

/// Whether a literal fits a type it shares a result column with: a branch
/// of `if_else`, an argument of `coalesce`/`fill_null`, a `shift` default.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Fit {
    /// The literal as a value of the type.
    Exactly(ScalarValue),
    /// The type holds no value equal to the literal.
    Inexact,
    /// Not a pair ltseq judges (a string, a Boolean, a number at a string
    /// type): the literal is DataFusion's to read, as before #225 (D-h).
    Unjudged,
}

/// How `literal` fits `context` (decision D-b on #225: a literal takes the
/// type of the values it shares a column with when that type holds it
/// exactly). At a float type a number literal is the nearest float of that
/// width, which is DataFusion's reading and the one float exception D-i
/// allows: a float column keeps float semantics. Everywhere else the
/// literal must be [`held`] exactly, a float by its binary value (D-j):
/// `0.5` fits a `decimal(5, 2)`, `0.1` (0.1000000000000000055…) does not.
pub(crate) fn fit(literal: &ScalarValue, context: &DataType) -> Fit {
    if literal.is_null() {
        return Fit::Unjudged;
    }
    if context.is_floating() {
        return match held(context, literal) {
            Holding::Unjudged => Fit::Unjudged,
            Holding::Exactly(value) => Fit::Exactly(value),
            Holding::Not(_) => match literal.cast_to(context) {
                Ok(nearest) if !nearest.is_null() => Fit::Exactly(nearest),
                _ => Fit::Inexact,
            },
        };
    }
    match held(context, literal) {
        Holding::Exactly(value) => Fit::Exactly(value),
        Holding::Not(_) => Fit::Inexact,
        Holding::Unjudged => Fit::Unjudged,
    }
}

/// What DataFusion's unification of a context type into a result type does
/// to the context's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Widening {
    /// The result type holds every value of the context type.
    Exact,
    /// A finer timestamp, which holds every instant of the context type
    /// that lies in its range: the widening a finer literal asks for
    /// (decision D-m on #225).
    Finer,
    /// Some values of the context type are lost.
    Lossy,
}

/// How `result` widens `context` ([`cast_class`], read as D-b and D-m read
/// it): a finer timestamp unit is accepted on range alone, every other
/// range or precision loss, and every change of kind, is lossy. A change
/// of zone is a change of kind, so a result column never takes a literal's
/// zone over the column's.
pub(crate) fn widening(context: &DataType, result: &DataType) -> Widening {
    match cast_class(context, result) {
        CastClass::Exact => Widening::Exact,
        CastClass::RangeOnly
            if matches!(
                (context, result),
                (DataType::Timestamp(..), DataType::Timestamp(..))
            ) =>
        {
            Widening::Finer
        }
        CastClass::RangeOnly
        | CastClass::Precision
        | CastClass::RangeAndPrecision
        | CastClass::Kind
        | CastClass::Unjudged => Widening::Lossy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use DataType as T;
    use Position::{Arithmetic, Comparison, Value};
    use ScalarValue as S;

    const NY: &str = "America/New_York";

    fn dec(value: i128, precision: u8, scale: i8) -> S {
        S::Decimal128(Some(value), precision, scale)
    }

    fn ts_us(value: i64, zone: Option<&str>) -> S {
        S::TimestampMicrosecond(Some(value), zone.map(Arc::from))
    }

    fn zoned_us() -> T {
        T::Timestamp(TimeUnit::Microsecond, Some(Arc::from(NY)))
    }

    const HOUR_US: i64 = 3_600_000_000;
    const JAN_1_2024_US: i64 = 1_704_067_200_000_000; // 2024-01-01 00:00 UTC

    #[test]
    fn readings() {
        let table: Vec<(S, T, Position, Option<S>)> = vec![
            // Decimal next to a float is that float, in every position.
            (
                dec(25, 2, 1),
                T::Float64,
                Comparison,
                Some(S::Float64(Some(2.5))),
            ),
            (
                dec(25, 2, 1),
                T::Float64,
                Arithmetic,
                Some(S::Float64(Some(2.5))),
            ),
            (dec(1, 1, 1), T::Float32, Value, Some(S::Float32(Some(0.1)))),
            // Naive datetime next to a zoned timestamp: wall-clock time there.
            (
                ts_us(JAN_1_2024_US, None),
                zoned_us(),
                Comparison,
                Some(ts_us(JAN_1_2024_US + 5 * HOUR_US, Some(NY))),
            ),
            (
                ts_us(JAN_1_2024_US, None),
                zoned_us(),
                Arithmetic,
                Some(ts_us(JAN_1_2024_US + 5 * HOUR_US, Some(NY))),
            ),
            // A date next to a zoned timestamp: local midnight at its unit.
            (
                S::Date32(Some(19723)),
                zoned_us(),
                Value,
                Some(ts_us(JAN_1_2024_US + 5 * HOUR_US, Some(NY))),
            ),
            // Left to DataFusion.
            (dec(25, 2, 1), T::Int64, Comparison, None),
            (dec(25, 2, 1), T::Decimal128(5, 2), Value, None),
            (
                ts_us(1, None),
                T::Timestamp(TimeUnit::Second, None),
                Comparison,
                None,
            ),
            (ts_us(1, Some("UTC")), zoned_us(), Comparison, None),
            (S::Date32(Some(1)), T::Date32, Comparison, None),
            (S::Int64(Some(1)), T::Utf8, Comparison, None),
            // NaN and infinity are floats next to a float, in every position.
            (S::Float64(Some(f64::NAN)), T::Float64, Comparison, None),
            (S::Float64(Some(f64::INFINITY)), T::Float32, Value, None),
            // In arithmetic a number next to an integer is DataFusion's float.
            (S::Float64(Some(f64::NAN)), T::Int64, Arithmetic, None),
            // A string is DataFusion's to read, also next to a number.
            (
                S::Utf8(Some("1.5".into())),
                T::Decimal128(5, 2),
                Value,
                None,
            ),
        ];
        for (literal, context, position, expected) in table {
            let expected = match expected {
                Some(value) => Reading::Value(value),
                None => Reading::Keep,
            };
            assert_eq!(
                interpret(&literal, &context, position, "column 'x'").unwrap(),
                expected,
                "{literal:?} next to {context} in {position:?}"
            );
        }
    }

    #[test]
    fn refusals() {
        let table: Vec<(S, T, Position, &str)> = vec![
            (dec(15, 2, 1), T::Utf8, Comparison, "column 'x' is a string"),
            (
                S::Date32(Some(1)),
                T::LargeUtf8,
                Value,
                "column 'x' is a string",
            ),
            (
                S::Date32(Some(1)),
                T::Int64,
                Comparison,
                "column 'x' is numeric",
            ),
            (ts_us(1, None), T::Float64, Value, "column 'x' is numeric"),
            (
                ts_us(1, None),
                T::Decimal128(5, 2),
                Comparison,
                "column 'x' is numeric",
            ),
            (
                dec(15, 2, 1),
                T::Date32,
                Value,
                "column 'x' is a date or timestamp",
            ),
            (
                S::Int64(Some(5)),
                T::Date32,
                Value,
                "column 'x' is a date or timestamp (Date32); use a date or datetime, not 5",
            ),
            (
                S::Float64(Some(1.5)),
                T::Date64,
                Value,
                "column 'x' is a date or timestamp",
            ),
            (
                S::Boolean(Some(true)),
                zoned_us(),
                Value,
                "column 'x' is a date or timestamp",
            ),
            // D-l: in every position, not only a value, and for every number.
            (
                S::Int64(Some(5)),
                T::Date32,
                Comparison,
                "column 'x' is a date or timestamp (Date32); use a date or datetime, not 5",
            ),
            (
                dec(25, 2, 1),
                T::Date32,
                Comparison,
                "use a date or datetime, not 2.5",
            ),
            (
                S::Int64(Some(5)),
                zoned_us(),
                Arithmetic,
                "use a date or datetime, not 5; dt.add() adds days",
            ),
            (
                S::Float64(Some(0.5)),
                T::Timestamp(TimeUnit::Second, None),
                Comparison,
                "column 'x' is a date or timestamp",
            ),
            (
                S::Date32(Some(1)),
                T::Int64,
                Arithmetic,
                "column 'x' is numeric (Int64); use a number, not a date or datetime",
            ),
            // D-j: NaN and infinity have no place among integers or decimals.
            (
                S::Float64(Some(f64::NAN)),
                T::Int64,
                Comparison,
                "column 'x' is Int64, which has no value for NaN",
            ),
            (
                S::Float64(Some(f64::NEG_INFINITY)),
                T::Decimal128(5, 2),
                Value,
                "column 'x' is Decimal128(5, 2), which has no value for -inf",
            ),
            (
                S::Float32(Some(f32::INFINITY)),
                T::UInt8,
                Comparison,
                "has no value for inf",
            ),
            (
                ts_us(1, Some("UTC")),
                T::Timestamp(TimeUnit::Microsecond, None),
                Arithmetic,
                "column 'x' is timezone-naive",
            ),
            // 2024-03-10 02:30 does not exist in New York; 2024-11-03 01:30 occurs twice.
            (
                ts_us(1_710_037_800_000_000, None),
                zoned_us(),
                Comparison,
                "2024-03-10 02:30:00 does not exist in America/New_York",
            ),
            (
                ts_us(1_730_597_400_000_000, None),
                zoned_us(),
                Value,
                "2024-11-03 01:30:00 is ambiguous in America/New_York",
            ),
        ];
        for (literal, context, position, message) in table {
            let err = interpret(&literal, &context, position, "column 'x'").unwrap_err();
            assert!(
                err.contains(message),
                "{literal:?} next to {context}: {err}"
            );
        }
    }

    #[test]
    fn a_zone_change_is_lossy_but_the_column_zone_holds_the_instant() {
        // tz A to tz B is a change of kind for the facts (review on #225),
        // so a shared result column never widens to the literal's zone; the
        // literal fits the column's zone as the same instant instead.
        let utc = T::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        assert_eq!(widening(&zoned_us(), &utc), Widening::Lossy);
        assert_eq!(widening(&utc, &zoned_us()), Widening::Lossy);
        assert_eq!(widening(&zoned_us(), &zoned_us()), Widening::Exact);
        assert_eq!(
            fit(&ts_us(JAN_1_2024_US, Some("UTC")), &zoned_us()),
            Fit::Exactly(ts_us(JAN_1_2024_US, Some(NY)))
        );
        assert_eq!(
            fit(&ts_us(JAN_1_2024_US, Some(NY)), &utc),
            Fit::Exactly(ts_us(JAN_1_2024_US, Some("UTC")))
        );
        // A finer unit in the same zone is the one widening D-m allows.
        let ns_ny = T::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(NY)));
        assert_eq!(widening(&zoned_us(), &ns_ny), Widening::Finer);
    }

    #[test]
    fn errors_write_literals_as_text() {
        let texts = [
            (dec(1236, 6, 3), "1.236"),
            (S::Date32(Some(19724)), "2024-01-02"),
            (
                S::TimestampMicrosecond(Some(1_704_175_200_000_001), None),
                "2024-01-02 06:00:00.000001",
            ),
            // An aware timestamp in its own zone's local time.
            (ts_us(0, Some(NY)), "1969-12-31 19:00:00 America/New_York"),
            (ts_us(0, Some("+09:00")), "1970-01-01 09:00:00 +09:00"),
            (S::Int64(Some(-1)), "-1"),
        ];
        for (literal, text) in texts {
            assert_eq!(literal_text(&literal), text);
        }
    }

    #[test]
    fn a_zone_offset_can_move_an_instant_past_the_literal_unit() {
        // pandas.Timestamp.max as a naive literal is 2262-04-11 23:47:16.854775807;
        // New York is west of UTC, so as wall-clock time there it is an
        // instant past the nanosecond range. It is not exact at microseconds:
        // a comparison places the instant, any other position refuses it.
        let max = S::TimestampNanosecond(Some(i64::MAX), None);
        let us_ny = T::Timestamp(TimeUnit::Microsecond, Some(Arc::from(NY)));
        assert!(matches!(
            interpret(&max, &us_ny, Comparison, "x").unwrap(),
            Reading::Instant(instant, TimeUnit::Nanosecond) if instant > i128::from(i64::MAX)
        ));
        assert!(interpret(&max, &us_ny, Value, "x")
            .unwrap_err()
            .contains("outside the range"));
        // A whole second earlier it is exact at microseconds.
        let late = S::TimestampNanosecond(Some(i64::MAX - 854_775_807), None);
        let read = interpret(&late, &us_ny, Value, "x").unwrap();
        assert!(
            matches!(
                read,
                Reading::Value(S::TimestampMicrosecond(Some(_), Some(_)))
            ),
            "{read:?}"
        );
    }
}
