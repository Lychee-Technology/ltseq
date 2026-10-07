//! Exact arithmetic on literal values: moving a number, date or timestamp
//! between the units or scales of two types without losing anything, or
//! saying it cannot be done ([`exact_cast`]); judging what a cast between
//! two types can lose ([`cast_loss`]); and [`place`], which finds where a
//! value falls among the values of a type.
//!
//! A number is an unscaled `i256` and a scale. A literal is at most a
//! 38-digit Decimal128, but the column it meets can be a decimal of any
//! width, up to a 76-digit Decimal256, and an `i256` holds the unscaled
//! value of every one of them.

use std::cmp::Ordering;

use datafusion::arrow::datatypes::{i256, DataType, TimeUnit};
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
    match operand {
        t if t.is_integer() || t.is_decimal() => {
            let (value, value_scale) = decimal_of(value)?;
            place_number(value, i32::from(value_scale), operand)
        }
        T::Timestamp(..) | T::Date32 | T::Date64 => {
            let (ticks, unit) = instant_of(value)?;
            place_instant(ticks, unit, operand)
        }
        _ => None,
    }
}

/// An instant, `ticks` of `unit` (an `i128`, so it may lie outside the `i64`
/// range of its unit), placed among the values of a timestamp or date type.
/// The values of a date type are days, each at its UTC midnight: a Date64
/// counts milliseconds, but Arrow requires a whole number of days.
pub(crate) fn place_instant(ticks: i128, unit: TimeUnit, operand: &DataType) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    match operand {
        T::Timestamp(to, zone) => Some(place_ticks(ticks, unit, *to, |t| {
            timestamp_scalar(*to, Some(t), zone.clone())
        })),
        T::Date32 | T::Date64 => {
            let per_day = i128::from(86_400 * ticks_per_second(unit));
            let days = ticks.div_euclid(per_day);
            let exact = ticks.rem_euclid(per_day) == 0;
            let date = match operand {
                T::Date32 => i32::try_from(days).ok().map(|days| S::Date32(Some(days))),
                _ => days
                    .checked_mul(86_400_000)
                    .and_then(|ms| i64::try_from(ms).ok())
                    .map(|ms| S::Date64(Some(ms))),
            };
            Some(match date {
                Some(date) if exact => Placement::Exact(date),
                Some(date) => Placement::Between(date),
                None => Placement::Beyond(days.cmp(&0)),
            })
        }
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
            t => {
                let (precision, scale) = decimal_parts(t)?;
                i32::from(precision) - i32::from(scale)
            }
        })
    };
    let scale = |t: &DataType| decimal_parts(t).map_or(0, |(_, scale)| i32::from(scale));
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
    let ((from_min, from_max), (to_min, to_max)) = (int_range(from), int_range(to));
    to_min <= from_min && from_max <= to_max
}

/// The smallest and largest value of an integer type (of `UInt64` for any
/// other type).
fn int_range(t: &DataType) -> (i128, i128) {
    use DataType as T;
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
}

/// What a cast to a type does to a literal's value ([`exact_cast`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cast {
    /// The value comes through unchanged, as this value of the type.
    Exact(ScalarValue),
    /// The type holds no value equal to it: the cast would round, truncate
    /// or overflow it.
    Inexact,
    /// Not a pair ltseq judges: a string or Boolean value, a value at a type
    /// of another kind (a number at a string type, at a date type), a NULL.
    /// What the value means there is DataFusion's reading, so it is left as
    /// written (decision D-h on #225 for strings).
    Unjudged,
}

/// What casting `value` to `to` does. ltseq judges the kinds it has exact
/// arithmetic for: a number at an integer or decimal type and a date or
/// timestamp at a date or timestamp type are [`place`]d, so a date type
/// holds a timestamp's instant only at a UTC midnight, as comparisons read
/// it. A float with a fraction is the number its shortest decimal text names
/// (`0.1` is 0.1), and an integral float its exact integer (`2.0**63` is
/// 2^63); the cast keeps it when it gives exactly that number. A round trip
/// would not do, since Arrow's float-to-decimal cast can turn `2.5` at scale
/// 33 into 2.50000000000000015216… and read it back as 2.5, and a round trip
/// between kinds says nothing: `"1.5"` comes back from a decimal as
/// `"1.50"`. A number cast to a float keeps the nearest float.
pub(crate) fn exact_cast(value: &ScalarValue, to: &DataType) -> Cast {
    use ScalarValue as S;
    if &value.data_type() == to {
        return Cast::Exact(value.clone());
    }
    // Placed rather than cast, since Arrow cannot rescale a decimal across a
    // gap of more than 38 digits (scale 38 to scale -1).
    if let Some(placed) = place(value, to) {
        return match placed {
            Placement::Exact(exact) => Cast::Exact(exact),
            Placement::Between(_) | Placement::Beyond(_) => Cast::Inexact,
        };
    }
    let float = match value {
        S::Float32(Some(v)) => Some((f64::from(*v), v.to_string())),
        S::Float64(Some(v)) => Some((*v, v.to_string())),
        _ => None,
    };
    let number = float.is_some() || decimal_of(value).is_some();
    if !number || !(to.is_floating() || to.is_integer() || to.is_decimal()) {
        return Cast::Unjudged;
    }
    let Some(cast) = value.cast_to(to).ok().filter(|cast| !cast.is_null()) else {
        return Cast::Inexact;
    };
    if to.is_floating() {
        return Cast::Exact(cast);
    }
    // A float at an integer or decimal type (any other number was placed).
    let exact = float
        .and_then(|(float, text)| float_number(float, &text))
        .and_then(|(value, scale)| place_number(value, scale, to));
    if exact == Some(Placement::Exact(cast.clone())) {
        Cast::Exact(cast)
    } else {
        Cast::Inexact
    }
}

/// The number a float means, as an unscaled integer and its scale: an
/// integral float its exact integer, any other float the number its
/// shortest decimal text `text` (no exponent) names. `None` for NaN, the
/// infinities and numbers of more digits than an `i256` holds, which no
/// decimal holds either.
fn float_number(float: f64, text: &str) -> Option<(i256, i32)> {
    if float.is_finite() && float.fract() == 0.0 {
        return i256::from_f64(float).map(|integer| (integer, 0));
    }
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let unscaled: i256 = format!("{whole}{fraction}").parse().ok()?;
    let unscaled = if negative { -unscaled } else { unscaled };
    Some((unscaled, i32::try_from(fraction.len()).ok()?))
}

/// The precision and scale of a decimal type of any width.
fn decimal_parts(t: &DataType) -> Option<(u8, i8)> {
    use DataType as T;
    match t {
        T::Decimal32(precision, scale)
        | T::Decimal64(precision, scale)
        | T::Decimal128(precision, scale)
        | T::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

/// An integer or decimal value as an unscaled integer and its scale.
fn decimal_of(value: &ScalarValue) -> Option<(i256, i8)> {
    use ScalarValue as S;
    Some(match value {
        S::Int8(Some(v)) => (i256::from(*v), 0),
        S::Int16(Some(v)) => (i256::from(*v), 0),
        S::Int32(Some(v)) => (i256::from(*v), 0),
        S::Int64(Some(v)) => (i256::from(*v), 0),
        S::UInt8(Some(v)) => (i256::from(i128::from(*v)), 0),
        S::UInt16(Some(v)) => (i256::from(i128::from(*v)), 0),
        S::UInt32(Some(v)) => (i256::from(i128::from(*v)), 0),
        S::UInt64(Some(v)) => (i256::from(i128::from(*v)), 0),
        S::Decimal32(Some(v), _, scale) => (i256::from(*v), *scale),
        S::Decimal64(Some(v), _, scale) => (i256::from(*v), *scale),
        S::Decimal128(Some(v), _, scale) => (i256::from(*v), *scale),
        S::Decimal256(Some(v), _, scale) => (*v, *scale),
        _ => return None,
    })
}

/// A non-null date or timestamp value as an instant, in ticks and their
/// unit: a timestamp's own (UTC, when it has a zone), a date's UTC midnight.
fn instant_of(value: &ScalarValue) -> Option<(i128, TimeUnit)> {
    use ScalarValue as S;
    let (ticks, unit) = match value {
        S::TimestampSecond(Some(v), _) => (*v, TimeUnit::Second),
        S::TimestampMillisecond(Some(v), _) => (*v, TimeUnit::Millisecond),
        S::TimestampMicrosecond(Some(v), _) => (*v, TimeUnit::Microsecond),
        S::TimestampNanosecond(Some(v), _) => (*v, TimeUnit::Nanosecond),
        S::Date64(Some(ms)) => (*ms, TimeUnit::Millisecond),
        S::Date32(Some(days)) => return Some((i128::from(*days) * 86_400, TimeUnit::Second)),
        _ => return None,
    };
    Some((i128::from(ticks), unit))
}

/// `10^exponent`, when an `i256` holds it (up to 10^76).
fn ten_to(exponent: u32) -> Option<i256> {
    i256::from(10).checked_pow(exponent)
}

/// `value / 10^value_scale` floored to an integer: the floor, and whether it is exact.
fn floor_at_scale(value: i256, value_scale: i32, scale: i32) -> Option<(i256, bool)> {
    if value_scale <= scale {
        let factor = ten_to((scale - value_scale).unsigned_abs())?;
        return value.checked_mul(factor).map(|v| (v, true));
    }
    // More than 76 digits finer than `scale`: 10^gap exceeds every i256, so
    // the value is within one step of zero.
    let Some(divisor) = ten_to((value_scale - scale).unsigned_abs()) else {
        return Some(if value.is_negative() {
            (i256::MINUS_ONE, false)
        } else {
            (i256::ZERO, value == i256::ZERO)
        });
    };
    // Division truncates toward zero; a negative remainder steps the floor down.
    let (quotient, remainder) = (value / divisor, value % divisor);
    Some(if remainder.is_negative() {
        (quotient - i256::ONE, false)
    } else {
        (quotient, remainder == i256::ZERO)
    })
}

/// The number `value / 10^value_scale` placed among the values of an
/// integer or decimal type.
fn place_number(value: i256, value_scale: i32, operand: &DataType) -> Option<Placement> {
    match decimal_parts(operand) {
        Some(_) => place_decimal(operand, value, value_scale),
        None => place_integer(operand, value, value_scale),
    }
}

/// The number `value / 10^value_scale` placed at a decimal type of any width.
fn place_decimal(operand: &DataType, value: i256, value_scale: i32) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    let (precision, scale) = decimal_parts(operand)?;
    // Every bound is below 10^precision, so it fits the operand's width.
    let bound = |v: i256| match operand {
        T::Decimal32(..) => S::Decimal32(
            v.to_i128().and_then(|v| i32::try_from(v).ok()),
            precision,
            scale,
        ),
        T::Decimal64(..) => S::Decimal64(
            v.to_i128().and_then(|v| i64::try_from(v).ok()),
            precision,
            scale,
        ),
        T::Decimal128(..) => S::Decimal128(v.to_i128(), precision, scale),
        _ => S::Decimal256(Some(v), precision, scale),
    };
    // A value of the type is below 10^precision in unscaled units.
    let limit = ten_to(u32::from(precision))?;
    Some(match floor_at_scale(value, value_scale, i32::from(scale)) {
        // Scaling up overflowed i256: far beyond the type.
        None => Placement::Beyond(value.cmp(&i256::ZERO)),
        Some((floor, _)) if floor >= limit => Placement::Beyond(Ordering::Greater),
        Some((floor, _)) if floor <= -limit => Placement::Beyond(Ordering::Less),
        Some((floor, true)) => Placement::Exact(bound(floor)),
        Some((floor, false)) => Placement::Between(bound(floor)),
    })
}

/// The number `value / 10^value_scale` placed at an integer type.
fn place_integer(operand: &DataType, value: i256, value_scale: i32) -> Option<Placement> {
    use DataType as T;
    use ScalarValue as S;
    if !operand.is_integer() {
        return None;
    }
    let (min, max) = int_range(operand);
    let bound = |v: i256| -> ScalarValue {
        let v = v.to_i128();
        match operand {
            T::Int8 => S::Int8(v.and_then(|v| i8::try_from(v).ok())),
            T::Int16 => S::Int16(v.and_then(|v| i16::try_from(v).ok())),
            T::Int32 => S::Int32(v.and_then(|v| i32::try_from(v).ok())),
            T::Int64 => S::Int64(v.and_then(|v| i64::try_from(v).ok())),
            T::UInt8 => S::UInt8(v.and_then(|v| u8::try_from(v).ok())),
            T::UInt16 => S::UInt16(v.and_then(|v| u16::try_from(v).ok())),
            T::UInt32 => S::UInt32(v.and_then(|v| u32::try_from(v).ok())),
            _ => S::UInt64(v.and_then(|v| u64::try_from(v).ok())),
        }
    };
    let (min, max) = (i256::from(min), i256::from(max));
    Some(match floor_at_scale(value, value_scale, 0) {
        None => Placement::Beyond(value.cmp(&i256::ZERO)),
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

    fn dec32(value: i32, precision: u8, scale: i8) -> ScalarValue {
        ScalarValue::Decimal32(Some(value), precision, scale)
    }

    fn dec64(value: i64, precision: u8, scale: i8) -> ScalarValue {
        ScalarValue::Decimal64(Some(value), precision, scale)
    }

    fn dec256(value: i256, precision: u8, scale: i8) -> ScalarValue {
        ScalarValue::Decimal256(Some(value), precision, scale)
    }

    fn ten(exponent: u32) -> i256 {
        ten_to(exponent).unwrap()
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
            // value: the value is within one step of zero.
            ((38, -1), 1, 38, Between(dec(0, 38, -1))),
            ((38, -1), -1, 38, Between(dec(-1, 38, -1))),
            ((38, -1), 0, 38, Exact(dec(0, 38, -1))),
            ((38, -38), 10i128.pow(38) - 1, 38, Between(dec(0, 38, -38))),
            ((38, -1), 15, 0, Between(dec(1, 38, -1))),
            ((38, 20), 10i128.pow(18), 0, Beyond(Greater)),
        ];
        for ((precision, scale), value, value_scale, expected) in table {
            let operand = DataType::Decimal128(precision, scale);
            assert_eq!(
                place_decimal(&operand, i256::from(value), value_scale),
                Some(expected),
                "{operand} {value}e-{value_scale}"
            );
        }
    }

    /// Columns can be decimals of any width: each bound is a value of the
    /// operand's own width, and a Decimal256 holds values past every i128.
    #[test]
    fn decimal_placements_at_every_width() {
        use DataType as T;
        let table = [
            (
                T::Decimal32(9, 2),
                i256::from(1236),
                3,
                Between(dec32(123, 9, 2)),
            ),
            (
                T::Decimal32(9, 2),
                i256::from(15),
                1,
                Exact(dec32(150, 9, 2)),
            ),
            (T::Decimal32(9, 2), ten(7), 0, Beyond(Greater)),
            (
                T::Decimal64(18, -2),
                i256::from(15),
                1,
                Between(dec64(0, 18, -2)),
            ),
            (
                T::Decimal64(18, -2),
                i256::from(-15),
                1,
                Between(dec64(-1, 18, -2)),
            ),
            (
                T::Decimal64(18, -2),
                i256::from(300),
                0,
                Exact(dec64(3, 18, -2)),
            ),
            (
                T::Decimal256(76, 20),
                i256::from(15),
                1,
                Exact(dec256(i256::from(15) * ten(19), 76, 20)),
            ),
            // 10^37 at scale 20 is 10^57 unscaled: past i128, within 76 digits.
            (
                T::Decimal256(76, 20),
                ten(37),
                0,
                Exact(dec256(ten(57), 76, 20)),
            ),
            (T::Decimal256(76, 20), ten(56), 0, Beyond(Greater)),
            (T::Decimal256(76, 20), -ten(56), 0, Beyond(Less)),
            // 1e-300 (a float's text has that many digits): more than 76
            // digits finer than the operand, within one step of zero.
            (
                T::Decimal256(76, 0),
                i256::ONE,
                300,
                Between(dec256(i256::ZERO, 76, 0)),
            ),
            (
                T::Decimal256(76, 0),
                i256::MINUS_ONE,
                300,
                Between(dec256(i256::MINUS_ONE, 76, 0)),
            ),
            (
                T::Decimal256(76, 0),
                i256::ZERO,
                300,
                Exact(dec256(i256::ZERO, 76, 0)),
            ),
        ];
        for (operand, value, value_scale, expected) in table {
            assert_eq!(
                place_decimal(&operand, value, value_scale),
                Some(expected),
                "{operand} {value}e-{value_scale}"
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
            // A Date64 holds days too: 06:00 is between two of them.
            (
                DataType::Date64,
                ts(Second, 86_400 + 6 * 3_600),
                Some(Between(S::Date64(Some(86_400_000)))),
            ),
            (
                DataType::Date64,
                ts(Millisecond, -1),
                Some(Between(S::Date64(Some(-86_400_000)))),
            ),
            (
                DataType::Date64,
                S::Date32(Some(19_723)),
                Some(Exact(S::Date64(Some(19_723 * 86_400_000)))),
            ),
            // An aware timestamp is its instant: midnight in UTC+9 is 15:00
            // UTC the day before.
            (
                DataType::Date32,
                timestamp_scalar(
                    Second,
                    Some(19_723 * 86_400 - 9 * 3_600),
                    Some("+09:00".into()),
                ),
                Some(Between(S::Date32(Some(19_722)))),
            ),
            (
                DataType::Date32,
                timestamp_scalar(Second, Some(19_723 * 86_400), Some("+09:00".into())),
                Some(Exact(S::Date32(Some(19_723)))),
            ),
            (DataType::Utf8, ts(Second, 1), None),
            (DataType::Date32, S::Int64(Some(5)), None),
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
            // Decimals of every width.
            (T::Decimal32(9, 2), T::Int64, Precision),
            (T::Decimal32(9, 2), T::Decimal64(18, 2), Loss::None),
            (T::Decimal64(18, 2), T::Decimal128(22, 2), Loss::None),
            (T::Decimal256(76, 0), T::Decimal128(38, 0), Precision),
            (T::Decimal128(38, 10), T::Decimal256(76, 10), Loss::None),
            (T::Int64, T::Decimal256(76, 20), Loss::None),
            (T::Int64, T::Decimal32(9, 0), Precision),
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
            // A Date64 holds only midnights (review of b6cc39f on #225:
            // 06:00 came back unchanged from Arrow's cast).
            (
                timestamp_scalar(Microsecond, Some(86_400_000_000 + 6 * 3_600_000_000), None),
                DataType::Date64,
                None,
            ),
            (
                timestamp_scalar(Microsecond, Some(86_400_000_000), None),
                DataType::Date64,
                Some(S::Date64(Some(86_400_000))),
            ),
            (
                S::Date32(Some(1)),
                DataType::Date64,
                Some(S::Date64(Some(86_400_000))),
            ),
            // An aware timestamp is its instant, not its local date: midnight
            // in UTC+9 is not a UTC midnight, 09:00 there is.
            (
                timestamp_scalar(Second, Some(86_400 - 9 * 3_600), Some("+09:00".into())),
                DataType::Date32,
                None,
            ),
            (
                timestamp_scalar(Second, Some(86_400), Some("+09:00".into())),
                DataType::Date32,
                Some(S::Date32(Some(1))),
            ),
        ];
        for (value, to, expected) in table {
            let expected = expected.map_or(Cast::Inexact, Cast::Exact);
            assert_eq!(exact_cast(&value, &to), expected, "{value:?} as {to}");
        }
    }

    /// Pairs ltseq has no exact arithmetic for are not judged: a round trip
    /// through Arrow's cast would refuse `"1.5"` at a decimal type (it comes
    /// back as `"1.50"`) and read an integer as days at a date type.
    #[test]
    fn other_kinds_are_not_judged() {
        use DataType as T;
        use ScalarValue as S;
        let text = |s: &str| S::Utf8(Some(s.to_string()));
        let table = [
            (text("1.5"), T::Decimal128(5, 2)),
            (text("05"), T::Int64),
            (text("abc"), T::Int64),
            (text("2024-01-01"), T::Date32),
            (S::Boolean(Some(true)), T::Int64),
            (S::Int64(Some(1)), T::Boolean),
            (S::Int64(Some(0)), T::Utf8),
            (S::Float64(Some(1.5)), T::Utf8),
            (S::Int64(Some(5)), T::Date32),
            (S::Int64(Some(5)), T::Timestamp(Microsecond, None)),
            (S::Date32(Some(1)), T::Int64),
            (S::Int64(None), T::Decimal128(5, 2)),
            (S::Null, T::Int64),
        ];
        for (value, to) in table {
            assert_eq!(exact_cast(&value, &to), Cast::Unjudged, "{value:?} as {to}");
        }
    }

    /// A float, integer or Decimal128 literal cast to a decimal column of
    /// each width: kept when the column holds it exactly (review F1 on
    /// #225), refused when the cast rounds or overflows.
    #[test]
    fn exact_casts_at_every_width() {
        use DataType as T;
        use ScalarValue as S;
        let table = [
            (
                S::Float64(Some(1.5)),
                T::Decimal32(9, 2),
                Some(dec32(150, 9, 2)),
            ),
            (
                S::Float64(Some(1.5)),
                T::Decimal64(18, 2),
                Some(dec64(150, 18, 2)),
            ),
            (
                S::Float64(Some(1.5)),
                T::Decimal256(20, 2),
                Some(dec256(i256::from(150), 20, 2)),
            ),
            (
                S::Float64(Some(1.5)),
                T::Decimal256(76, 20),
                Some(dec256(i256::from(15) * ten(19), 76, 20)),
            ),
            (
                S::Float32(Some(1.5)),
                T::Decimal64(18, 2),
                Some(dec64(150, 18, 2)),
            ),
            (S::Float64(Some(1.236)), T::Decimal32(9, 2), None),
            (S::Float64(Some(1.236)), T::Decimal64(18, 2), None),
            (S::Float64(Some(1.236)), T::Decimal256(20, 2), None),
            (
                S::Float64(Some(1.236)),
                T::Decimal256(76, 20),
                Some(dec256(i256::from(1236) * ten(17), 76, 20)),
            ),
            (S::Float64(Some(1e8)), T::Decimal32(9, 2), None),
            (S::Float64(Some(2.5)), T::Decimal256(76, 33), None),
            // An integral float past i128 is its exact integer.
            (
                S::Float64(Some(2f64.powi(200))),
                T::Decimal256(76, 0),
                Some(dec256(i256::ONE << 200u8, 76, 0)),
            ),
            (S::Float64(Some(1e300)), T::Decimal256(76, 0), None),
            (S::Float64(Some(f64::NAN)), T::Decimal256(76, 0), None),
            (
                S::Int64(Some(1)),
                T::Decimal256(76, 70),
                Some(dec256(ten(70), 76, 70)),
            ),
            (S::Int64(Some(1)), T::Decimal32(9, 9), None),
            (dec(15, 2, 1), T::Decimal32(9, 2), Some(dec32(150, 9, 2))),
            (dec(15, 2, 1), T::Decimal64(18, -2), None),
            (
                dec(10i128.pow(37), 38, 0),
                T::Decimal256(76, 38),
                Some(dec256(ten(75), 76, 38)),
            ),
            (dec(10i128.pow(37), 38, 0), T::Decimal256(76, 40), None),
        ];
        for (value, to, expected) in table {
            let expected = expected.map_or(Cast::Inexact, Cast::Exact);
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
