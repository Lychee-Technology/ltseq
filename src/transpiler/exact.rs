//! Exact arithmetic on literal values: moving a value between the units or
//! scales of two types without losing anything, or saying it cannot be done
//! ([`exact_cast`]); judging what a cast between two types can lose
//! ([`cast_loss`]); and [`place`], which finds where a value falls among the
//! values of a type.

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

/// What a cast from one type to another can lose, for every value of the
/// first type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Loss {
    /// Nothing: every value comes through unchanged.
    None,
    /// Only values outside the target's range (a timestamp or date cast to
    /// a finer timestamp unit, whose `i64` covers fewer years).
    Range,
    /// Digits or fractions (a decimal to fewer integer digits or a smaller
    /// scale, a timestamp to a coarser unit, a float to an exact type).
    Precision,
}

/// What casting a value of `from` to `to` can lose. A cast to a float type
/// counts as lossless: a float result holds the nearest float, which is
/// what DataFusion computes for a float column. Pairs this does not judge
/// count as lossless too, leaving them to DataFusion.
pub(crate) fn cast_loss(from: &DataType, to: &DataType) -> Loss {
    use DataType as T;
    if from == to || from == &T::Null || to.is_floating() {
        return Loss::None;
    }
    let digits = |t: &DataType| -> Option<i32> {
        Some(match t {
            T::Int8 | T::UInt8 => 3,
            T::Int16 | T::UInt16 => 5,
            T::Int32 | T::UInt32 => 10,
            T::Int64 => 19,
            T::UInt64 => 20,
            T::Decimal128(precision, scale) => i32::from(*precision) - i32::from(*scale),
            _ => return None,
        })
    };
    let scale = |t: &DataType| match t {
        T::Decimal128(_, scale) => i32::from(*scale),
        _ => 0,
    };
    let rank = |unit: &TimeUnit| ticks_per_second(*unit);
    match (from, to) {
        (f, t) if f.is_integer() && t.is_integer() => {
            if int_range_within(f, t) {
                Loss::None
            } else {
                Loss::Precision
            }
        }
        // An integer or decimal and a decimal: enough integer digits and scale.
        (f, t) if digits(f).is_some() && digits(t).is_some() => {
            if digits(t) >= digits(f) && scale(t) >= scale(f) {
                Loss::None
            } else {
                Loss::Precision
            }
        }
        (f, _) if f.is_floating() => Loss::Precision,
        (T::Timestamp(from_unit, _), T::Timestamp(to_unit, _)) => {
            match rank(to_unit).cmp(&rank(from_unit)) {
                Ordering::Greater => Loss::Range,
                Ordering::Less => Loss::Precision,
                Ordering::Equal => Loss::None,
            }
        }
        (T::Date32 | T::Date64, T::Timestamp(TimeUnit::Nanosecond, _)) => Loss::Range,
        (T::Timestamp(..) | T::Date64, T::Date32) | (T::Timestamp(..), T::Date64) => {
            Loss::Precision
        }
        _ => Loss::None,
    }
}

/// Whether every value of integer type `from` is a value of integer type `to`.
fn int_range_within(from: &DataType, to: &DataType) -> bool {
    use DataType as T;
    let range = |t: &DataType| -> (i128, i128) {
        match t {
            T::Int8 => (i8::MIN.into(), i8::MAX.into()),
            T::Int16 => (i16::MIN.into(), i16::MAX.into()),
            T::Int32 => (i32::MIN.into(), i32::MAX.into()),
            T::Int64 => (i64::MIN.into(), i64::MAX.into()),
            T::UInt8 => (0, u8::MAX.into()),
            T::UInt16 => (0, u16::MAX.into()),
            T::UInt32 => (0, u32::MAX.into()),
            _ => (0, u64::MAX.into()),
        }
    };
    let ((from_min, from_max), (to_min, to_max)) = (range(from), range(to));
    to_min <= from_min && from_max <= to_max
}

/// `value` as a value of `to`, when the cast keeps it: an integer, decimal,
/// date or timestamp that comes back unchanged from the cast. A float with
/// a fraction is the number its shortest decimal text names (`0.1` is
/// 0.1), and an integral float its exact integer (`2.0**63` is 2^63); a
/// round trip would not do, since Arrow's float-to-decimal cast can turn
/// `2.5` at scale 33 into 2.50000000000000015216… and read it back as 2.5.
/// A cast to a float keeps the nearest float.
pub(crate) fn exact_cast(value: &ScalarValue, to: &DataType) -> Option<ScalarValue> {
    use ScalarValue as S;
    if &value.data_type() == to {
        return Some(value.clone());
    }
    // A number at a number type: placed exactly, since Arrow cannot rescale a
    // decimal across a gap of more than 38 digits (scale 38 to scale -1).
    if decimal_of(value).is_some() && (to.is_integer() || matches!(to, DataType::Decimal128(..))) {
        return match place(value, to)? {
            Placement::Exact(exact) => Some(exact),
            Placement::Between(_) | Placement::Beyond(_) => None,
        };
    }
    let cast = value.cast_to(to).ok().filter(|cast| !cast.is_null())?;
    if to.is_floating() {
        return Some(cast);
    }
    let kept = match value {
        S::Float32(Some(v)) => float_is(f64::from(*v), &v.to_string(), &cast),
        S::Float64(Some(v)) => float_is(*v, &v.to_string(), &cast),
        _ => cast.cast_to(&value.data_type()).ok().as_ref() == Some(value),
    };
    kept.then_some(cast)
}

/// Whether the float `float` (whose shortest decimal text is `text`) is the
/// integer or decimal `value`.
fn float_is(float: f64, text: &str, value: &ScalarValue) -> bool {
    if float.is_finite() && float.fract() == 0.0 && float.abs() < 2f64.powi(127) {
        // An integral float below 2^127 converts to i128 exactly.
        return same_number(&(float as i128).to_string(), value);
    }
    same_number(text, value)
}

/// Whether the decimal text `text` (no exponent) and the integer or decimal
/// `value` are the same number.
fn same_number(text: &str, value: &ScalarValue) -> bool {
    let Some((value, value_scale)) = decimal_of(value) else {
        return false;
    };
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let Ok(unscaled) = format!("{whole}{fraction}").parse::<i128>() else {
        return false;
    };
    let unscaled = if negative { -unscaled } else { unscaled };
    let Ok(text_scale) = i32::try_from(fraction.len()) else {
        return false;
    };
    // Compare at the larger scale.
    let scale = text_scale.max(i32::from(value_scale));
    let at = |v: i128, s: i32| {
        10i128
            .checked_pow((scale - s).unsigned_abs())
            .and_then(|f| v.checked_mul(f))
    };
    matches!((at(unscaled, text_scale), at(value, i32::from(value_scale))), (Some(a), Some(b)) if a == b)
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
    fn cast_losses() {
        use DataType as T;
        use Loss::{Precision, Range};
        let table = [
            (T::Decimal128(5, 2), T::Decimal128(36, 33), Loss::None),
            (T::Decimal128(5, 2), T::Decimal128(38, 33), Loss::None),
            (T::Decimal128(38, 10), T::Decimal128(38, 20), Precision),
            (T::Decimal128(38, -1), T::Decimal128(38, 38), Precision),
            (T::Decimal128(14, 7), T::Decimal128(38, 33), Precision),
            (T::Int64, T::Decimal128(20, 0), Loss::None),
            (T::Int64, T::Decimal128(38, 33), Precision),
            (T::UInt64, T::Decimal128(22, 2), Loss::None),
            (T::Int32, T::Int64, Loss::None),
            (T::Int64, T::UInt64, Precision),
            (T::Int64, T::Float64, Loss::None),
            (T::Float64, T::Decimal128(30, 15), Precision),
            (
                T::Timestamp(Second, None),
                T::Timestamp(Microsecond, None),
                Range,
            ),
            (
                T::Timestamp(Nanosecond, None),
                T::Timestamp(Second, None),
                Precision,
            ),
            (T::Date32, T::Timestamp(Nanosecond, None), Range),
            (T::Date32, T::Timestamp(Second, None), Loss::None),
            (T::Timestamp(Second, None), T::Date32, Precision),
            (T::Utf8, T::Utf8View, Loss::None),
        ];
        for (from, to, expected) in table {
            assert_eq!(cast_loss(&from, &to), expected, "{from} -> {to}");
        }
    }

    #[test]
    fn exact_casts() {
        use ScalarValue as S;
        let table = [
            // Arrow's cast gives 2.50000000000000015216… at scale 33.
            (S::Float64(Some(2.5)), DataType::Decimal128(38, 33), None),
            (
                S::Float64(Some(2.5)),
                DataType::Decimal128(30, 15),
                Some(dec(25 * 10i128.pow(14), 30, 15)),
            ),
            (
                S::Float64(Some(0.1)),
                DataType::Decimal128(30, 15),
                Some(dec(10i128.pow(14), 30, 15)),
            ),
            (
                S::Float64(Some(2f64.powi(63))),
                DataType::UInt64,
                Some(S::UInt64(Some(1 << 63))),
            ),
            (S::Float64(Some(1.5)), DataType::Int64, None),
            (
                S::Float64(Some(2.0)),
                DataType::Int64,
                Some(S::Int64(Some(2))),
            ),
            (
                S::Float64(Some(-0.0)),
                DataType::Int64,
                Some(S::Int64(Some(0))),
            ),
            (S::Float64(Some(f64::NAN)), DataType::Int64, None),
            (S::Float64(Some(1.236)), DataType::Decimal128(5, 2), None),
            (
                S::Float64(Some(1.1)),
                DataType::Decimal128(5, 2),
                Some(dec(110, 5, 2)),
            ),
            (
                S::Float64(Some(0.1)),
                DataType::Float32,
                Some(S::Float32(Some(0.1))),
            ),
            (dec(15, 2, 1), DataType::Int64, None),
            (
                dec(1 << 63, 19, 0),
                DataType::UInt64,
                Some(S::UInt64(Some(1 << 63))),
            ),
            (dec(1 << 63, 19, 0), DataType::Int64, None),
            (S::Int64(Some(-1)), DataType::UInt64, None),
            (S::Int64(Some(300)), DataType::Int8, None),
            (dec(15, 2, 1), DataType::Decimal128(10, -1), None),
            (
                dec(20, 2, 0),
                DataType::Decimal128(10, -1),
                Some(dec(2, 10, -1)),
            ),
            // 39 digits between the scales: past what Arrow can rescale.
            (
                dec(0, 1, 38),
                DataType::Decimal128(38, -1),
                Some(dec(0, 38, -1)),
            ),
            (dec(1, 1, 38), DataType::Decimal128(38, -1), None),
            (
                timestamp_scalar(Microsecond, Some(86_400_000_000), None),
                DataType::Date32,
                Some(S::Date32(Some(1))),
            ),
            (
                timestamp_scalar(Microsecond, Some(86_400_000_001), None),
                DataType::Date32,
                None,
            ),
            (
                timestamp_scalar(Microsecond, Some(1_500_000), None),
                DataType::Timestamp(Second, None),
                None,
            ),
        ];
        for (value, to, expected) in table {
            assert_eq!(exact_cast(&value, &to), expected, "{value:?} as {to}");
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
