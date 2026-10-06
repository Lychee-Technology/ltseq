//! Exact arithmetic on literal values: moving a value between the units or
//! scales of two types without losing anything, or saying it cannot be done,
//! and [`place`], which finds where a value falls among the values of a type.

use std::cmp::Ordering;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;

use crate::types::timestamp_scalar;

/// Where a value falls among the values a type can hold.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Placement {
    /// The value itself, as a value of the type.
    Exact(ScalarValue),
    /// Strictly between this value of the type (the floor) and the next one.
    Between(ScalarValue),
    /// Above (`Greater`) or below (`Less`) every value of the type.
    Beyond(Ordering),
}

/// `value` placed among the values of `operand`, or `None` when ltseq does
/// not place this kind of value at this type (the value is then left to
/// DataFusion). Numbers (integers and decimals) place at integer and decimal
/// types; dates and timestamps place at date and timestamp types, a
/// timestamp at its instant (or wall-clock time, when both are naive) and a
/// date at its UTC midnight.
pub(crate) fn place(value: &ScalarValue, operand: &DataType) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    match operand {
        T::Decimal128(precision, scale) => {
            let (value, value_scale) = decimal_of(value)?;
            Some(place_decimal((*precision, *scale), value, value_scale))
        }
        t if t.is_integer() => {
            let (value, value_scale) = decimal_of(value)?;
            place_integer(t, value, value_scale)
        }
        T::Timestamp(..) | T::Date32 | T::Date64 => match (timestamp_of(value), value, operand) {
            (Some((ticks, unit)), _, _) => place_instant(ticks.into(), unit, operand),
            // A date at a timestamp type is its (UTC) midnight.
            (None, S::Date32(Some(days)), T::Timestamp(unit, _)) => {
                let midnight = i128::from(*days) * i128::from(86_400 * ticks_per_second(*unit));
                place_instant(midnight, *unit, operand)
            }
            _ => None,
        },
        _ => None,
    }
}

/// An instant, `ticks` of `unit` (an `i128`, so it may lie outside the `i64`
/// range of its unit), placed among the values of a timestamp or date type.
pub(crate) fn place_instant(ticks: i128, unit: TimeUnit, operand: &DataType) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    match operand {
        T::Timestamp(to, zone) => Some(place_ticks(ticks, unit, *to, |t| {
            timestamp_scalar(*to, Some(t), zone.clone())
        })),
        T::Date32 => {
            let per_day = i128::from(86_400 * ticks_per_second(unit));
            let days = ticks.div_euclid(per_day);
            let exact = ticks.rem_euclid(per_day) == 0;
            Some(match i32::try_from(days) {
                Ok(days) if exact => Placement::Exact(S::Date32(Some(days))),
                Ok(days) => Placement::Between(S::Date32(Some(days))),
                Err(_) => Placement::Beyond(days.cmp(&0)),
            })
        }
        // A Date64 counts milliseconds since the epoch.
        T::Date64 => Some(place_ticks(ticks, unit, TimeUnit::Millisecond, |ms| {
            S::Date64(Some(ms))
        })),
        _ => None,
    }
}

/// An integer or decimal value as an unscaled integer and its scale.
fn decimal_of(value: &ScalarValue) -> Option<(i128, i8)> {
    use ScalarValue as S;
    Some(match value {
        S::Int8(Some(v)) => (i128::from(*v), 0),
        S::Int16(Some(v)) => (i128::from(*v), 0),
        S::Int32(Some(v)) => (i128::from(*v), 0),
        S::Int64(Some(v)) => (i128::from(*v), 0),
        S::UInt8(Some(v)) => (i128::from(*v), 0),
        S::UInt16(Some(v)) => (i128::from(*v), 0),
        S::UInt32(Some(v)) => (i128::from(*v), 0),
        S::UInt64(Some(v)) => (i128::from(*v), 0),
        S::Decimal128(Some(v), _, scale) => (*v, *scale),
        _ => return None,
    })
}

/// A non-null timestamp value: its ticks and unit.
fn timestamp_of(value: &ScalarValue) -> Option<(i64, TimeUnit)> {
    use ScalarValue as S;
    match value {
        S::TimestampSecond(Some(v), _) => Some((*v, TimeUnit::Second)),
        S::TimestampMillisecond(Some(v), _) => Some((*v, TimeUnit::Millisecond)),
        S::TimestampMicrosecond(Some(v), _) => Some((*v, TimeUnit::Microsecond)),
        S::TimestampNanosecond(Some(v), _) => Some((*v, TimeUnit::Nanosecond)),
        _ => None,
    }
}

/// `value / 10^value_scale` floored to an integer: the floor, and whether it is exact.
fn floor_at_scale(value: i128, value_scale: i32, scale: i32) -> Option<(i128, bool)> {
    if value_scale <= scale {
        let factor = 10i128.checked_pow((scale - value_scale).unsigned_abs())?;
        return value.checked_mul(factor).map(|v| (v, true));
    }
    // More than 38 digits finer than `scale`: 10^gap exceeds every i128, so
    // the value is within one step of zero.
    Some(
        match 10i128.checked_pow((value_scale - scale).unsigned_abs()) {
            Some(divisor) => (value.div_euclid(divisor), value.rem_euclid(divisor) == 0),
            None => (if value < 0 { -1 } else { 0 }, value == 0),
        },
    )
}

/// The decimal `value / 10^value_scale` placed at a decimal type
/// `(precision, scale)`.
fn place_decimal(operand: (u8, i8), value: i128, value_scale: i8) -> Placement {
    let (precision, scale) = operand;
    let bound = |v: i128| ScalarValue::Decimal128(Some(v), precision, scale);
    // A value of the type is below 10^precision in unscaled units.
    let limit = 10i128.pow(u32::from(precision));
    match floor_at_scale(value, i32::from(value_scale), i32::from(scale)) {
        // Scaling up overflowed i128: far beyond the type.
        None => Placement::Beyond(value.cmp(&0)),
        Some((floor, _)) if floor >= limit => Placement::Beyond(Ordering::Greater),
        Some((floor, _)) if floor <= -limit => Placement::Beyond(Ordering::Less),
        Some((floor, true)) => Placement::Exact(bound(floor)),
        Some((floor, false)) => Placement::Between(bound(floor)),
    }
}

/// The decimal `value / 10^value_scale` placed at an integer type.
fn place_integer(operand: &DataType, value: i128, value_scale: i8) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    let (min, max): (i128, i128) = match operand {
        T::Int8 => (i8::MIN.into(), i8::MAX.into()),
        T::Int16 => (i16::MIN.into(), i16::MAX.into()),
        T::Int32 => (i32::MIN.into(), i32::MAX.into()),
        T::Int64 => (i64::MIN.into(), i64::MAX.into()),
        T::UInt8 => (0, u8::MAX.into()),
        T::UInt16 => (0, u16::MAX.into()),
        T::UInt32 => (0, u32::MAX.into()),
        T::UInt64 => (0, u64::MAX.into()),
        _ => return None,
    };
    let bound = |v: i128| -> ScalarValue {
        match operand {
            T::Int8 => S::Int8(i8::try_from(v).ok()),
            T::Int16 => S::Int16(i16::try_from(v).ok()),
            T::Int32 => S::Int32(i32::try_from(v).ok()),
            T::Int64 => S::Int64(i64::try_from(v).ok()),
            T::UInt8 => S::UInt8(u8::try_from(v).ok()),
            T::UInt16 => S::UInt16(u16::try_from(v).ok()),
            T::UInt32 => S::UInt32(u32::try_from(v).ok()),
            _ => S::UInt64(u64::try_from(v).ok()),
        }
    };
    Some(match floor_at_scale(value, i32::from(value_scale), 0) {
        None => Placement::Beyond(value.cmp(&0)),
        Some((floor, _)) if floor > max => Placement::Beyond(Ordering::Greater),
        Some((floor, _)) if floor < min => Placement::Beyond(Ordering::Less),
        Some((floor, true)) => Placement::Exact(bound(floor)),
        Some((floor, false)) => Placement::Between(bound(floor)),
    })
}

/// `value` ticks of `from` placed among the ticks of `to`; `bound` builds a
/// value of the type from its ticks.
fn place_ticks(
    value: i128,
    from: TimeUnit,
    to: TimeUnit,
    bound: impl Fn(i64) -> ScalarValue,
) -> Placement {
    let (from, to) = (
        i128::from(ticks_per_second(from)),
        i128::from(ticks_per_second(to)),
    );
    let (ticks, exact) = if from > to {
        let ratio = from / to;
        (value.div_euclid(ratio), value.rem_euclid(ratio) == 0)
    } else {
        (value.saturating_mul(to / from), true)
    };
    match i64::try_from(ticks) {
        Ok(ticks) if exact => Placement::Exact(bound(ticks)),
        Ok(floor) => Placement::Between(bound(floor)),
        Err(_) => Placement::Beyond(ticks.cmp(&0)),
    }
}

pub(crate) fn ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// `value` ticks of `from` as ticks of `to`, when that is exact and fits an
/// `i64`. `value` is an `i128` because an instant computed from a literal
/// (a zone offset applied to wall-clock time) can lie outside the `i64`
/// range of one unit and inside that of a coarser one.
pub(crate) fn exact_ticks(value: i128, from: TimeUnit, to: TimeUnit) -> Option<i64> {
    let (from, to) = (
        i128::from(ticks_per_second(from)),
        i128::from(ticks_per_second(to)),
    );
    let ticks = if from >= to {
        let ratio = from / to;
        (value % ratio == 0).then_some(value / ratio)?
    } else {
        value.checked_mul(to / from)?
    };
    i64::try_from(ticks).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use Ordering::{Greater, Less};
    use Placement::{Between, Beyond, Exact};
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};

    fn dec(value: i128, precision: u8, scale: i8) -> ScalarValue {
        ScalarValue::Decimal128(Some(value), precision, scale)
    }

    /// The decimal table the earlier #225 branch checked against an exact
    /// `Fraction` oracle, for `place_decimal`.
    #[test]
    fn decimal_placements() {
        let table = [
            ((5, 2), 1236, 3, Between(dec(123, 5, 2))),
            ((5, 2), 1230, 3, Exact(dec(123, 5, 2))),
            (
                (38, 10),
                1_234_567_890_123_456_789,
                19,
                Between(dec(1_234_567_890, 38, 10)),
            ),
            (
                (38, 10),
                -1_234_567_890_123_456_789,
                19,
                Between(dec(-1_234_567_891, 38, 10)),
            ),
            (
                (38, 10),
                15 * 10i128.pow(18),
                19,
                Exact(dec(15 * 10i128.pow(9), 38, 10)),
            ),
            ((38, 10), 10i128.pow(38) - 1, 0, Beyond(Greater)),
            ((38, 10), -(10i128.pow(38) - 1), 0, Beyond(Less)),
            ((20, 0), 15 * 10i128.pow(30), 31, Between(dec(1, 20, 0))),
            ((20, 0), -15 * 10i128.pow(30), 31, Between(dec(-2, 20, 0))),
            ((20, 0), 2 * 10i128.pow(30), 30, Exact(dec(2, 20, 0))),
            // A negative-scale operand 39 or more digits coarser than the
            // value: 10^gap exceeds i128, and the value is within one step of zero.
            ((38, -1), 1, 38, Between(dec(0, 38, -1))),
            ((38, -1), -1, 38, Between(dec(-1, 38, -1))),
            ((38, -1), 0, 38, Exact(dec(0, 38, -1))),
            ((38, -38), 10i128.pow(38) - 1, 38, Between(dec(0, 38, -38))),
            ((38, -1), 15, 0, Between(dec(1, 38, -1))),
            ((38, 20), 10i128.pow(18), 0, Beyond(Greater)),
        ];
        for (operand, value, scale, expected) in table {
            assert_eq!(
                place_decimal(operand, value, scale),
                expected,
                "{operand:?} {value}e-{scale}"
            );
        }
    }

    #[test]
    fn integer_placements() {
        use ScalarValue as S;
        let table = [
            (
                DataType::Int64,
                dec(15, 2, 1),
                Some(Between(S::Int64(Some(1)))),
            ),
            (
                DataType::Int64,
                dec(-15, 2, 1),
                Some(Between(S::Int64(Some(-2)))),
            ),
            (
                DataType::Int64,
                dec(20, 2, 1),
                Some(Exact(S::Int64(Some(2)))),
            ),
            (
                DataType::Int64,
                dec(i128::from(i64::MAX) + 1, 20, 0),
                Some(Beyond(Greater)),
            ),
            (DataType::UInt64, S::Int64(Some(-1)), Some(Beyond(Less))),
            (
                DataType::UInt64,
                dec(i128::from(u64::MAX), 20, 0),
                Some(Exact(S::UInt64(Some(u64::MAX)))),
            ),
            (DataType::Int8, S::Int64(Some(300)), Some(Beyond(Greater))),
            (
                DataType::Int8,
                S::Int64(Some(-128)),
                Some(Exact(S::Int8(Some(-128)))),
            ),
            (
                DataType::Int8,
                dec(1275, 4, 1),
                Some(Between(S::Int8(Some(127)))),
            ),
            (DataType::Int64, S::Float64(Some(1.5)), None),
        ];
        for (operand, value, expected) in table {
            assert_eq!(place(&value, &operand), expected, "{value:?} at {operand}");
        }
    }

    #[test]
    fn temporal_placements() {
        use ScalarValue as S;
        let ts = |unit, v| timestamp_scalar(unit, Some(v), None);
        let table = [
            // 1.5 s at seconds: between 1 and 2.
            (
                DataType::Timestamp(Second, None),
                ts(Microsecond, 1_500_000),
                Some(Between(ts(Second, 1))),
            ),
            (
                DataType::Timestamp(Second, None),
                ts(Microsecond, -1_500_000),
                Some(Between(ts(Second, -2))),
            ),
            (
                DataType::Timestamp(Microsecond, None),
                ts(Second, 2),
                Some(Exact(ts(Microsecond, 2_000_000))),
            ),
            // Year 2300 is past the nanosecond range.
            (
                DataType::Timestamp(Nanosecond, None),
                ts(Microsecond, 10_413_792_000_000_000),
                Some(Beyond(Greater)),
            ),
            (
                DataType::Timestamp(Microsecond, None),
                S::Date32(Some(1)),
                Some(Exact(ts(Microsecond, 86_400_000_000))),
            ),
            // 06:00 on day 19723 is between that date and the next.
            (
                DataType::Date32,
                ts(Second, 19_723 * 86_400 + 6 * 3_600),
                Some(Between(S::Date32(Some(19_723)))),
            ),
            (
                DataType::Date32,
                ts(Second, 19_723 * 86_400),
                Some(Exact(S::Date32(Some(19_723)))),
            ),
            (
                DataType::Date32,
                ts(Second, -1),
                Some(Between(S::Date32(Some(-1)))),
            ),
            (
                DataType::Date64,
                ts(Second, 86_400),
                Some(Exact(S::Date64(Some(86_400_000)))),
            ),
            (DataType::Utf8, ts(Second, 1), None),
        ];
        for (operand, value, expected) in table {
            assert_eq!(place(&value, &operand), expected, "{value:?} at {operand}");
        }
    }

    #[test]
    fn exact_ticks_table() {
        let table = [
            (1_500_000_i128, Microsecond, Second, None),
            (2_000_000, Microsecond, Second, Some(2)),
            (-2_000_000, Microsecond, Second, Some(-2)),
            (-1_500_000, Microsecond, Second, None),
            (3, Second, Millisecond, Some(3_000)),
            (i128::from(i64::MAX), Second, Nanosecond, None),
            // Past the i64 range at nanoseconds, inside it at microseconds.
            (
                i128::from(i64::MAX) * 1_000,
                Nanosecond,
                Microsecond,
                Some(i64::MAX),
            ),
        ];
        for (value, from, to, expected) in table {
            assert_eq!(
                exact_ticks(value, from, to),
                expected,
                "{value} {from:?} -> {to:?}"
            );
        }
    }
}
