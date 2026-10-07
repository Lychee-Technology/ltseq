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

use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;

use super::exact::{exact_ticks, ticks_per_second};
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
        (t, _) if kinds_checked && temporal && t.is_numeric() => Err(format!(
            "{name} is numeric ({context}); use a number, not a date or datetime"
        )),
        (T::Date32 | T::Date64 | T::Timestamp(..), _)
            if position == Position::Value
                && (literal.data_type().is_numeric() || matches!(literal, S::Boolean(_))) =>
        {
            Err(format!(
                "{name} is a date or timestamp ({context}); use a date or datetime, not {}",
                literal_text(literal)
            ))
        }
        (T::Timestamp(unit, zone), _) => zoned(literal, *unit, zone.as_deref(), position, name),
        _ => Ok(Reading::Keep),
    }
}

/// A date or timestamp literal against a timestamp of `unit` in `zone`:
/// the zone rules of the table above.
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
            (S::Date32(Some(1)), T::Int64, Arithmetic, None),
            (dec(25, 2, 1), T::Date32, Comparison, None),
            (S::Int64(Some(5)), T::Date32, Comparison, None),
            (S::Int64(Some(5)), zoned_us(), Arithmetic, None),
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
