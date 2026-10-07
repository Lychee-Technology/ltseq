//! Facts about Arrow's types and values, with no policy in them: what a
//! cast from one type to another can lose over every value of the first
//! ([`cast_class`]), and whether a type holds a given value, with where the
//! value falls among the type's values when it does not ([`holds`],
//! [`hold_instant`]). What a fact means for a literal next to a column is
//! decided in `literal_policy`.
//!
//! Both are total over Arrow's types, and a pair this module has no
//! arithmetic for is `Unjudged`, never exact. A float is the binary value
//! its bits encode (`0.1_f64` is 0.1000000000000000055511151231257827…), a
//! decimal its unscaled integer over a power of ten, an integer itself, a
//! date its UTC midnight and a timestamp its instant; every comparison and
//! floor is computed on those as exact rationals, so no step rounds. A
//! literal is at most a 38-digit Decimal128, but the column it meets can be
//! a decimal of any width, up to a 76-digit Decimal256.
//!
//! A Date64 is judged two ways, each as Arrow does. [`cast_class`] describes
//! Arrow's cast, which moves a Date64 to and from a millisecond timestamp
//! unchanged and does not enforce whole days, so there a Date64 counts
//! milliseconds. [`holds`] admits only whole days, the values the Arrow
//! format specifies, so a literal with a time of day is never placed at a
//! date. The two meet nowhere: DataFusion never unifies a timestamp into a
//! date, so no gate asks what a timestamp loses cast to a Date64.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::datatypes::{i256, ArrowPrimitiveType, DataType, Float16Type, TimeUnit};
use datafusion::scalar::ScalarValue;
use num_bigint::{BigInt, Sign};
use num_traits::{One, ToPrimitive, Zero};

use crate::types::timestamp_scalar;

/// Where a value falls among the values of a type that does not hold it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Placement {
    /// Strictly between this value of the type (the floor) and the next one.
    Between(ScalarValue),
    /// Above (`Greater`) or below (`Less`) every value of the type.
    Beyond(Ordering),
    /// Not a number: unordered against every value of the type.
    NotANumber,
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

/// The ticks of one second at a unit.
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

/// What Arrow's cast from one type to another does to the values of the
/// first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastClass {
    Exact,
    /// Some values of `from` lie outside `to`, or Arrow's cast cannot
    /// compute them; every value inside is kept exactly.
    RangeOnly,
    /// Some values of `from` fall between two values of `to` and round.
    Precision,
    RangeAndPrecision,
    /// The types are of different kinds (a number and a string, a number
    /// and a date, a naive and a zoned timestamp, two zones of a
    /// timestamp): the cast changes what the value is, not only how it is
    /// stored.
    Kind,
    /// Two types of a kind this module does not judge (strings, booleans,
    /// lists, ...).
    Unjudged,
}

/// Whether a type holds a value exactly.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Holding {
    /// The value, as a value of the type.
    Exactly(ScalarValue),
    /// The type has no such value; where the value falls among its values.
    Not(Placement),
    /// This module does not judge this value at this type.
    Unjudged,
}

/// Arrow's half-precision float, reached through its Arrow type.
type Half = <Float16Type as ArrowPrimitiveType>::Native;

/// An IEEE 754 binary format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FloatFormat {
    Half,
    Single,
    Double,
}

impl FloatFormat {
    /// Significand bits, counting the hidden one.
    fn significand(self) -> i64 {
        match self {
            Self::Half => 11,
            Self::Single => 24,
            Self::Double => 53,
        }
    }

    /// Every finite value is below `2^max_exponent`.
    fn max_exponent(self) -> i64 {
        match self {
            Self::Half => 16,
            Self::Single => 128,
            Self::Double => 1024,
        }
    }

    /// The smallest subnormal is `2^min_exponent`; no value has a finer bit.
    fn min_exponent(self) -> i64 {
        match self {
            Self::Half => -24,
            Self::Single => -149,
            Self::Double => -1074,
        }
    }

    /// The largest finite value, `(2^significand - 1) * 2^(max_exponent - significand)`.
    fn max_finite(self) -> BigInt {
        ((BigInt::one() << self.significand()) - 1) << (self.max_exponent() - self.significand())
    }

    /// A value of this format, held in an `f64`, as a scalar of the format.
    fn scalar(self, value: f64) -> ScalarValue {
        match self {
            Self::Half => ScalarValue::Float16(Some(Half::from_f64(value))),
            Self::Single => ScalarValue::Float32(Some(value as f32)),
            Self::Double => ScalarValue::Float64(Some(value)),
        }
    }

    /// The value of this format just above `value`, a non-negative finite
    /// value of it below the largest.
    fn next_up(self, value: f64) -> f64 {
        match self {
            Self::Half => Half::from_bits(Half::from_f64(value).to_bits() + 1).to_f64(),
            Self::Single => f64::from((value as f32).next_up()),
            Self::Double => value.next_up(),
        }
    }

    /// `value`, positive and at most the largest finite value, rounded down
    /// to this format: a mantissa below `2^significand`, the exponent of its
    /// unit, and whether rounding lost nothing.
    fn floor(self, value: &Rational) -> (u64, i64, bool) {
        let unit = (value.floor_log2() - (self.significand() - 1)).max(self.min_exponent());
        let (mantissa, exact) = value.times_two_to(-unit).floor();
        let mantissa = mantissa
            .to_u64()
            .expect("a value below 2^max_exponent has a mantissa below 2^significand");
        (mantissa, unit, exact)
    }
}

/// `mantissa * 2^exponent` as an `f64`, for a mantissa below `2^53` and a
/// product that is a finite value of f64 (every value of a smaller format is).
fn binary(mantissa: u64, exponent: i64) -> f64 {
    if mantissa == 0 {
        return 0.0;
    }
    let width = i64::from(u64::BITS - mantissa.leading_zeros());
    let shift = 53 - width;
    if exponent - shift >= -1074 {
        let mantissa = mantissa << shift;
        let biased = (exponent - shift + 1075) as u64;
        f64::from_bits((biased << 52) | (mantissa & ((1 << 52) - 1)))
    } else {
        // Subnormal: the unit is 2^-1074 and the exponent field is zero.
        f64::from_bits(mantissa << (exponent + 1074))
    }
}

/// A number type, by what decides exactness.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Number {
    Int {
        signed: bool,
        bits: u32,
    },
    Float(FloatFormat),
    Decimal {
        precision: u8,
        scale: i8,
        storage_bits: u32,
    },
}

/// A date or timestamp type.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Instant {
    Date32,
    Date64,
    Timestamp(TimeUnit, Option<Arc<str>>),
}

enum Kind {
    Null,
    Number(Number),
    Instant(Instant),
    Other,
}

/// The type whose values a type holds: a dictionary's or run-end-encoded
/// type's value type, otherwise the type itself.
fn value_type(t: &DataType) -> &DataType {
    match t {
        DataType::Dictionary(_, values) => value_type(values),
        DataType::RunEndEncoded(_, values) => value_type(values.data_type()),
        _ => t,
    }
}

fn kind_of(t: &DataType) -> Kind {
    use DataType as T;
    let int = |signed, bits| Kind::Number(Number::Int { signed, bits });
    let float = |format| Kind::Number(Number::Float(format));
    let decimal = |precision: &u8, scale: &i8, storage_bits| {
        Kind::Number(Number::Decimal {
            precision: *precision,
            scale: *scale,
            storage_bits,
        })
    };
    match t {
        T::Null => Kind::Null,
        T::Int8 => int(true, 8),
        T::Int16 => int(true, 16),
        T::Int32 => int(true, 32),
        T::Int64 => int(true, 64),
        T::UInt8 => int(false, 8),
        T::UInt16 => int(false, 16),
        T::UInt32 => int(false, 32),
        T::UInt64 => int(false, 64),
        T::Float16 => float(FloatFormat::Half),
        T::Float32 => float(FloatFormat::Single),
        T::Float64 => float(FloatFormat::Double),
        T::Decimal32(precision, scale) => decimal(precision, scale, 32),
        T::Decimal64(precision, scale) => decimal(precision, scale, 64),
        T::Decimal128(precision, scale) => decimal(precision, scale, 128),
        T::Decimal256(precision, scale) => decimal(precision, scale, 256),
        T::Date32 => Kind::Instant(Instant::Date32),
        T::Date64 => Kind::Instant(Instant::Date64),
        T::Timestamp(unit, zone) => Kind::Instant(Instant::Timestamp(*unit, zone.clone())),
        T::Dictionary(_, values) => kind_of(values),
        T::RunEndEncoded(_, values) => kind_of(values.data_type()),
        T::Boolean
        | T::Time32(_)
        | T::Time64(_)
        | T::Duration(_)
        | T::Interval(_)
        | T::Binary
        | T::FixedSizeBinary(_)
        | T::LargeBinary
        | T::BinaryView
        | T::Utf8
        | T::LargeUtf8
        | T::Utf8View
        | T::List(_)
        | T::ListView(_)
        | T::FixedSizeList(..)
        | T::LargeList(_)
        | T::LargeListView(_)
        | T::Struct(_)
        | T::Union(..)
        | T::Map(..) => Kind::Other,
    }
}

/// What casting `from` to `to` can lose. A dictionary or run-end-encoded
/// type casts as its value type.
pub(crate) fn cast_class(from: &DataType, to: &DataType) -> CastClass {
    let (from, to) = (value_type(from), value_type(to));
    if from == to {
        return CastClass::Exact;
    }
    match (kind_of(from), kind_of(to)) {
        (Kind::Null, _) => CastClass::Exact,
        (_, Kind::Null) => CastClass::Kind,
        (Kind::Other, Kind::Other) => CastClass::Unjudged,
        (Kind::Other, _) | (_, Kind::Other) => CastClass::Kind,
        (Kind::Number(from), Kind::Number(to)) => number_cast(&from, &to),
        (Kind::Instant(from), Kind::Instant(to)) => instant_cast(&from, &to),
        (Kind::Number(_), Kind::Instant(_)) | (Kind::Instant(_), Kind::Number(_)) => {
            CastClass::Kind
        }
    }
}

fn classify(range: bool, precision: bool) -> CastClass {
    match (range, precision) {
        (false, false) => CastClass::Exact,
        (true, false) => CastClass::RangeOnly,
        (false, true) => CastClass::Precision,
        (true, true) => CastClass::RangeAndPrecision,
    }
}

fn number_cast(from: &Number, to: &Number) -> CastClass {
    use Number::{Decimal, Float, Int};
    if from == to {
        return CastClass::Exact;
    }
    let (from_min, from_max) = bounds(from);
    let (to_min, to_max) = bounds(to);
    // NaN and the infinities have no place among the integers and decimals.
    let not_a_number = matches!((from, to), (Float(_), Int { .. } | Decimal { .. }));
    let range = from_max > to_max || from_min < to_min || not_a_number || scaled_up(from, to);
    // A value `to` does not hold loses precision only when it is in range;
    // every larger unheld value is out of range once the least one is.
    let precision = least_unheld(from, to).is_some_and(|value| value <= to_max);
    classify(range, precision)
}

/// Whether Arrow's cast scales a negative-scale decimal past its storage.
///
/// Arrow scales such a decimal up in its own storage integer before
/// converting it to an integer, so the scaled maximum must fit that storage
/// too (`arrow_cast::cast_decimal_to_integer`).
fn scaled_up(from: &Number, to: &Number) -> bool {
    match (from, to) {
        (
            Number::Decimal {
                precision,
                scale,
                storage_bits,
            },
            Number::Int { .. },
        ) if *scale < 0 => {
            let scaled_max =
                (pow10(u32::from(*precision)) - 1) * pow10(u32::from(scale.unsigned_abs()));
            scaled_max > (BigInt::one() << (storage_bits - 1)) - 1
        }
        _ => false,
    }
}

/// The least positive value of `from` that `to` does not hold, disregarding
/// the range of `to`; `None` when `to` holds every value of `from` in range.
///
/// Held-ness is symmetric under negation, so the positive values suffice.
fn least_unheld(from: &Number, to: &Number) -> Option<Rational> {
    use Number::{Decimal, Float, Int};
    match (from, to) {
        (Int { .. }, Int { .. }) => None,
        // An integer of more magnitude bits than the significand rounds,
        // first at the odd number past it.
        (Int { signed, bits }, Float(format)) => {
            let magnitude_bits = i64::from(if *signed { bits - 1 } else { *bits });
            (magnitude_bits > format.significand())
                .then(|| Rational::integer((BigInt::one() << format.significand()) + 1))
        }
        // A negative scale keeps only multiples of a power of ten.
        (Int { .. }, Decimal { scale, .. }) => {
            (*scale < 0).then(|| Rational::integer(BigInt::one()))
        }
        // The smallest subnormal is a fraction.
        (Float(format), Int { .. }) => Some(Rational::dyadic(BigInt::one(), format.min_exponent())),
        // A finer bit than `to` has, else more significant bits at `to`'s
        // finest exponent.
        (Float(from), Float(to)) => {
            if from.min_exponent() < to.min_exponent() {
                Some(Rational::dyadic(BigInt::one(), from.min_exponent()))
            } else if from.significand() > to.significand() {
                Some(Rational::dyadic(
                    (BigInt::one() << to.significand()) + 1,
                    from.min_exponent(),
                ))
            } else {
                None
            }
        }
        // The smallest subnormal, `2^min_exponent`, needs that many decimal
        // places.
        (Float(format), Decimal { scale, .. }) => (i64::from(*scale) < -format.min_exponent())
            .then(|| Rational::dyadic(BigInt::one(), format.min_exponent())),
        // The finest step of a positive scale is a fraction.
        (Decimal { scale, .. }, Int { .. }) => {
            (*scale > 0).then(|| Rational::scaled(BigInt::one(), *scale))
        }
        (Decimal { scale: from, .. }, Decimal { scale: to, .. }) => {
            (from > to).then(|| Rational::scaled(BigInt::one(), *from))
        }
        // Every value is `n * 10^-scale`: for a positive scale the finest
        // step is not dyadic; otherwise, with `k = -scale`, the value
        // `n * 10^k` has odd part `odd(n) * 5^k`, held only below
        // `2^significand`, and the least `n` past that bound is odd.
        (
            Decimal {
                precision, scale, ..
            },
            Float(format),
        ) => {
            if *scale > 0 {
                return Some(Rational::scaled(BigInt::one(), *scale));
            }
            let k = u32::from(scale.unsigned_abs());
            let five_k = BigInt::from(5).pow(k);
            let bound = BigInt::one() << format.significand();
            // The least integer `n` with `n * 5^k >= 2^significand`.
            let mut n: BigInt = (&bound + &five_k - BigInt::one()) / &five_k;
            if !n.bit(0) {
                n += 1;
            }
            (n < pow10(u32::from(*precision))).then(|| Rational::integer(n * pow10(k)))
        }
    }
}

/// The least and greatest finite value of a number type.
fn bounds(number: &Number) -> (Rational, Rational) {
    match number {
        Number::Int { signed, bits } => {
            let (min, max) = int_bounds(*signed, *bits);
            (Rational::integer(min), Rational::integer(max))
        }
        Number::Float(format) => {
            let max = Rational::integer(format.max_finite());
            (max.negated(), max)
        }
        Number::Decimal {
            precision, scale, ..
        } => {
            let max = Rational::scaled(pow10(u32::from(*precision)) - 1, *scale);
            (max.negated(), max)
        }
    }
}

fn int_bounds(signed: bool, bits: u32) -> (BigInt, BigInt) {
    if signed {
        let magnitude = BigInt::one() << (bits - 1);
        (-&magnitude, magnitude - 1)
    } else {
        (BigInt::zero(), (BigInt::one() << bits) - 1)
    }
}

fn instant_cast(from: &Instant, to: &Instant) -> CastClass {
    // A naive timestamp is a wall-clock time and a zoned one an instant
    // shown in its zone. Arrow's cast between a naive and a zoned
    // timestamp reads the one as the other in some zone, and its cast
    // between two zones keeps the instant but changes the wall-clock
    // reading of every value: both change what a value means, so both are
    // a change of kind here, and what a zone means for a literal is
    // `literal_policy`'s to decide.
    if zone(from) != zone(to) {
        return CastClass::Kind;
    }
    let precision = resolution(from) > resolution(to);
    let range = span_in_nanoseconds(from) > span_in_nanoseconds(to);
    classify(range, precision)
}

fn zone(instant: &Instant) -> Option<&str> {
    match instant {
        Instant::Timestamp(_, zone) => zone.as_deref(),
        Instant::Date32 | Instant::Date64 => None,
    }
}

/// Finer units rank higher; a Date64 counts milliseconds.
fn resolution(instant: &Instant) -> u8 {
    match instant {
        Instant::Date32 => 0,
        Instant::Timestamp(TimeUnit::Second, _) => 1,
        Instant::Date64 | Instant::Timestamp(TimeUnit::Millisecond, _) => 2,
        Instant::Timestamp(TimeUnit::Microsecond, _) => 3,
        Instant::Timestamp(TimeUnit::Nanosecond, _) => 4,
    }
}

/// The greatest instant of the type, in nanoseconds past the epoch.
fn span_in_nanoseconds(instant: &Instant) -> i128 {
    match instant {
        Instant::Date32 => i128::from(i32::MAX) * 86_400 * 1_000_000_000,
        Instant::Date64 => i128::from(i64::MAX) * 1_000_000,
        Instant::Timestamp(unit, _) => {
            i128::from(i64::MAX) * i128::from(1_000_000_000 / ticks_per_second(*unit))
        }
    }
}

/// Whether `to` holds `value`: the value as a value of `to` when it does,
/// where it falls among the values of `to` when it does not.
///
/// Numbers (integers, floats, decimals) are judged at number types, dates
/// and timestamps at date and timestamp types, by the value itself: a float
/// by the binary value its bits encode, a date at its UTC midnight, a
/// timestamp at its instant (or its wall-clock time, when both it and the
/// type are naive). A naive value at a zoned type, or a zoned one at a
/// naive type, is not judged: reading one as the other is policy. Nor are
/// strings, booleans and the other kinds, except that every type holds its
/// own values and the typed null.
pub(crate) fn holds(to: &DataType, value: &ScalarValue) -> Holding {
    let to = value_type(to);
    if let ScalarValue::Dictionary(_, value) = value {
        return holds(to, value);
    }
    if value.data_type() == *to {
        return Holding::Exactly(value.clone());
    }
    if value.is_null() {
        return ScalarValue::try_new_null(to).map_or(Holding::Unjudged, Holding::Exactly);
    }
    match kind_of(to) {
        Kind::Number(Number::Float(format)) => match float_of(value) {
            // Both signed zeros are values of every float format.
            Some(zero) if zero == 0.0 => Holding::Exactly(format.scalar(zero)),
            _ => real_of(value).map_or(Holding::Unjudged, |real| at_float(format, &real)),
        },
        Kind::Number(Number::Int { signed, bits }) => match real_of(value).map(finite) {
            None => Holding::Unjudged,
            Some(Err(not_finite)) => not_finite,
            Some(Ok(value)) => at_int(to, signed, bits, &value),
        },
        Kind::Number(Number::Decimal {
            precision, scale, ..
        }) => match real_of(value).map(finite) {
            None => Holding::Unjudged,
            Some(Err(not_finite)) => not_finite,
            Some(Ok(value)) => at_decimal(to, precision, scale, &value),
        },
        Kind::Instant(instant) => at_instant(to, &instant, value),
        Kind::Null | Kind::Other => Holding::Unjudged,
    }
}

/// A number value, read exactly.
enum Real {
    Finite(Rational),
    NotANumber,
    /// `Greater` is positive infinity.
    Infinite(Ordering),
}

/// A finite value, or the holding a non-finite one gets at a type whose
/// values are all finite numbers.
fn finite(real: Real) -> Result<Rational, Holding> {
    match real {
        Real::Finite(value) => Ok(value),
        Real::NotANumber => Err(Holding::Not(Placement::NotANumber)),
        Real::Infinite(sign) => Err(Holding::Not(Placement::Beyond(sign))),
    }
}

fn float_of(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float16(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Float32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Float64(Some(v)) => Some(*v),
        _ => None,
    }
}

/// An integer, float or decimal value as the number it is.
fn real_of(value: &ScalarValue) -> Option<Real> {
    if let Some(float) = float_of(value) {
        return Some(if float.is_nan() {
            Real::NotANumber
        } else if float.is_infinite() {
            Real::Infinite(float.partial_cmp(&0.0).unwrap_or(Ordering::Equal))
        } else {
            Real::Finite(binary_value(float))
        });
    }
    let (unscaled, scale) = decimal_of(value)?;
    Some(Real::Finite(Rational::scaled(big(unscaled), scale)))
}

/// The value a finite `f64` encodes: `(-1)^sign * mantissa * 2^exponent`,
/// read from its bits.
fn binary_value(float: f64) -> Rational {
    let bits = float.to_bits();
    let exponent_field = ((bits >> 52) & 0x7ff) as i64;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, exponent) = if exponent_field == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), exponent_field - 1075)
    };
    let mantissa = if bits >> 63 == 1 {
        -BigInt::from(mantissa)
    } else {
        BigInt::from(mantissa)
    };
    Rational::dyadic(mantissa, exponent)
}

fn at_float(format: FloatFormat, value: &Real) -> Holding {
    use Ordering::{Greater, Less};
    let value = match value {
        Real::NotANumber => return Holding::Exactly(format.scalar(f64::NAN)),
        Real::Infinite(Greater) => return Holding::Exactly(format.scalar(f64::INFINITY)),
        Real::Infinite(_) => return Holding::Exactly(format.scalar(f64::NEG_INFINITY)),
        Real::Finite(value) => value,
    };
    if value.is_zero() {
        return Holding::Exactly(format.scalar(0.0));
    }
    let negative = value.is_negative();
    let magnitude = value.abs();
    if magnitude > Rational::integer(format.max_finite()) {
        return Holding::Not(Placement::Beyond(if negative { Less } else { Greater }));
    }
    let (mantissa, exponent, exact) = format.floor(&magnitude);
    let floor = binary(mantissa, exponent);
    match (exact, negative) {
        (true, false) => Holding::Exactly(format.scalar(floor)),
        (true, true) => Holding::Exactly(format.scalar(-floor)),
        (false, false) => Holding::Not(Placement::Between(format.scalar(floor))),
        // Below zero, the floor is the negated ceiling of the magnitude.
        (false, true) => Holding::Not(Placement::Between(format.scalar(-format.next_up(floor)))),
    }
}

fn at_int(to: &DataType, signed: bool, bits: u32, value: &Rational) -> Holding {
    let (floor, exact) = value.floor();
    let (min, max) = int_bounds(signed, bits);
    if floor < min {
        return Holding::Not(Placement::Beyond(Ordering::Less));
    }
    // A fraction past the greatest value floors to it yet lies beyond.
    if floor > max || (floor == max && !exact) {
        return Holding::Not(Placement::Beyond(Ordering::Greater));
    }
    match (int_scalar(to, &floor), exact) {
        (Some(held), true) => Holding::Exactly(held),
        (Some(floor), false) => Holding::Not(Placement::Between(floor)),
        (None, _) => Holding::Unjudged,
    }
}

fn at_decimal(to: &DataType, precision: u8, scale: i8, value: &Rational) -> Holding {
    let (floor, exact) = value.times_ten_to(i32::from(scale)).floor();
    let limit = pow10(u32::from(precision)) - 1;
    if floor < -&limit {
        return Holding::Not(Placement::Beyond(Ordering::Less));
    }
    // A fraction past the greatest value floors to it yet lies beyond.
    if floor > limit || (floor == limit && !exact) {
        return Holding::Not(Placement::Beyond(Ordering::Greater));
    }
    match (decimal_scalar(to, &floor), exact) {
        (Some(held), true) => Holding::Exactly(held),
        (Some(floor), false) => Holding::Not(Placement::Between(floor)),
        (None, _) => Holding::Unjudged,
    }
}

fn at_instant(to: &DataType, instant: &Instant, value: &ScalarValue) -> Holding {
    let Some((ticks, unit)) = instant_of(value) else {
        return Holding::Unjudged;
    };
    if let Instant::Timestamp(_, zone) = instant {
        if zone.is_some() != zoned_value(value) {
            return Holding::Unjudged;
        }
    }
    hold_instant(ticks, unit, to)
}

fn zoned_value(value: &ScalarValue) -> bool {
    use ScalarValue as S;
    matches!(
        value,
        S::TimestampSecond(_, Some(_))
            | S::TimestampMillisecond(_, Some(_))
            | S::TimestampMicrosecond(_, Some(_))
            | S::TimestampNanosecond(_, Some(_))
    )
}

/// An instant, `ticks` of `unit` (an `i128`, so it may lie outside the `i64`
/// range of its unit), at a timestamp or date type. The values of a date
/// type are days, each at its UTC midnight: a Date64 counts milliseconds,
/// but Arrow requires a whole number of days.
pub(crate) fn hold_instant(ticks: i128, unit: TimeUnit, to: &DataType) -> Holding {
    use DataType as T;
    use ScalarValue as S;
    match to {
        T::Timestamp(to_unit, zone) => hold_ticks(ticks, unit, *to_unit, |t| {
            timestamp_scalar(*to_unit, Some(t), zone.clone())
        }),
        T::Date32 | T::Date64 => {
            let per_day = i128::from(86_400 * ticks_per_second(unit));
            let days = ticks.div_euclid(per_day);
            let exact = ticks.rem_euclid(per_day) == 0;
            let date = match to {
                T::Date32 => i32::try_from(days).ok().map(|days| S::Date32(Some(days))),
                _ => days
                    .checked_mul(86_400_000)
                    .and_then(|ms| i64::try_from(ms).ok())
                    .map(|ms| S::Date64(Some(ms))),
            };
            match date {
                Some(date) if exact => Holding::Exactly(date),
                Some(date) => Holding::Not(Placement::Between(date)),
                None => Holding::Not(Placement::Beyond(days.cmp(&0))),
            }
        }
        _ => Holding::Unjudged,
    }
}

fn hold_ticks(
    value: i128,
    from: TimeUnit,
    to: TimeUnit,
    bound: impl Fn(i64) -> ScalarValue,
) -> Holding {
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
        Ok(ticks) if exact => Holding::Exactly(bound(ticks)),
        Ok(floor) => Holding::Not(Placement::Between(bound(floor))),
        Err(_) => Holding::Not(Placement::Beyond(ticks.cmp(&0))),
    }
}

fn int_scalar(to: &DataType, value: &BigInt) -> Option<ScalarValue> {
    use ScalarValue as S;
    Some(match to {
        DataType::Int8 => S::Int8(Some(value.to_i8()?)),
        DataType::Int16 => S::Int16(Some(value.to_i16()?)),
        DataType::Int32 => S::Int32(Some(value.to_i32()?)),
        DataType::Int64 => S::Int64(Some(value.to_i64()?)),
        DataType::UInt8 => S::UInt8(Some(value.to_u8()?)),
        DataType::UInt16 => S::UInt16(Some(value.to_u16()?)),
        DataType::UInt32 => S::UInt32(Some(value.to_u32()?)),
        DataType::UInt64 => S::UInt64(Some(value.to_u64()?)),
        _ => return None,
    })
}

fn decimal_scalar(to: &DataType, unscaled: &BigInt) -> Option<ScalarValue> {
    use ScalarValue as S;
    Some(match to {
        DataType::Decimal32(p, s) => S::Decimal32(Some(unscaled.to_i32()?), *p, *s),
        DataType::Decimal64(p, s) => S::Decimal64(Some(unscaled.to_i64()?), *p, *s),
        DataType::Decimal128(p, s) => S::Decimal128(Some(unscaled.to_i128()?), *p, *s),
        DataType::Decimal256(p, s) => S::Decimal256(Some(wide(unscaled)?), *p, *s),
        _ => return None,
    })
}

fn big(value: i256) -> BigInt {
    BigInt::from_signed_bytes_le(&value.to_le_bytes())
}

/// `value` as an `i256`, when it fits.
fn wide(value: &BigInt) -> Option<i256> {
    let bytes = value.to_signed_bytes_le();
    if bytes.len() > 32 {
        return None;
    }
    let fill = if value.sign() == Sign::Minus { 0xff } else { 0 };
    let mut padded = [fill; 32];
    padded[..bytes.len()].copy_from_slice(&bytes);
    Some(i256::from_le_bytes(padded))
}

fn pow10(exponent: u32) -> BigInt {
    BigInt::from(10).pow(exponent)
}

/// An exact rational number, `num / den` with `den` positive.
///
/// Fractions are not reduced, so equality is by value, not by representation.
#[derive(Debug, Clone)]
struct Rational {
    num: BigInt,
    den: BigInt,
}

impl PartialEq for Rational {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Rational {}

impl Rational {
    fn integer(num: BigInt) -> Self {
        Self {
            num,
            den: BigInt::one(),
        }
    }

    /// `unscaled * 10^-scale`.
    fn scaled(unscaled: BigInt, scale: i8) -> Self {
        Self::integer(unscaled).times_ten_to(-i32::from(scale))
    }

    /// `mantissa * 2^exponent`.
    fn dyadic(mantissa: BigInt, exponent: i64) -> Self {
        Self::integer(mantissa).times_two_to(exponent)
    }

    fn times_ten_to(&self, exponent: i32) -> Self {
        let factor = pow10(exponent.unsigned_abs());
        if exponent >= 0 {
            Self {
                num: &self.num * factor,
                den: self.den.clone(),
            }
        } else {
            Self {
                num: self.num.clone(),
                den: &self.den * factor,
            }
        }
    }

    fn times_two_to(&self, exponent: i64) -> Self {
        if exponent >= 0 {
            Self {
                num: &self.num << exponent,
                den: self.den.clone(),
            }
        } else {
            Self {
                num: self.num.clone(),
                den: &self.den << -exponent,
            }
        }
    }

    fn negated(&self) -> Self {
        Self {
            num: -&self.num,
            den: self.den.clone(),
        }
    }

    fn abs(&self) -> Self {
        Self {
            num: BigInt::from_biguint(Sign::Plus, self.num.magnitude().clone()),
            den: self.den.clone(),
        }
    }

    fn is_zero(&self) -> bool {
        self.num.is_zero()
    }

    fn is_negative(&self) -> bool {
        self.num.sign() == Sign::Minus
    }

    /// The greatest integer not above the value, and whether it is the value.
    fn floor(&self) -> (BigInt, bool) {
        let (quotient, remainder) = (&self.num / &self.den, &self.num % &self.den);
        if remainder.is_zero() {
            (quotient, true)
        } else if remainder.sign() == Sign::Minus {
            (quotient - 1, false)
        } else {
            (quotient, false)
        }
    }

    /// `floor(log2(value))`, of a positive value.
    fn floor_log2(&self) -> i64 {
        let estimate = self.num.bits() as i64 - self.den.bits() as i64;
        let at_least = if estimate >= 0 {
            self.num >= &self.den << estimate
        } else {
            &self.num << -estimate >= self.den
        };
        if at_least {
            estimate
        } else {
            estimate - 1
        }
    }
}

impl PartialOrd for Rational {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Rational {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.num * &other.den).cmp(&(&other.num * &self.den))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use Ordering::{Greater, Less};
    use Placement::{Between, Beyond};
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

    /// `10^exponent` as an `i256` (up to 10^76).
    fn ten(exponent: u32) -> i256 {
        i256::from(10).checked_pow(exponent).unwrap()
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

    // -----------------------------------------------------------------------
    // Facts: cast_class and holds
    // -----------------------------------------------------------------------

    fn exactly(value: ScalarValue) -> Holding {
        Holding::Exactly(value)
    }

    fn between(floor: ScalarValue) -> Holding {
        Holding::Not(Between(floor))
    }

    fn beyond(side: Ordering) -> Holding {
        Holding::Not(Beyond(side))
    }

    fn half(value: f64) -> ScalarValue {
        ScalarValue::Float16(Some(Half::from_f64(value)))
    }

    fn big_decimal(digits: &str, precision: u8, scale: i8) -> ScalarValue {
        let unscaled: BigInt = digits.parse().unwrap();
        dec256(wide(&unscaled).unwrap(), precision, scale)
    }

    fn zoned_ts(unit: TimeUnit, v: i64, zone: &str) -> ScalarValue {
        timestamp_scalar(unit, Some(v), Some(zone.into()))
    }

    /// Every number pair is judged by its bounds and granularity: an integer
    /// of more magnitude bits than a float's significand rounds, a decimal
    /// with a fractional scale is not dyadic, and a negative-scale decimal
    /// must also fit its own storage integer once Arrow scales it up.
    #[test]
    fn cast_classes_between_numbers() {
        use CastClass::{Exact, Precision, RangeAndPrecision, RangeOnly};
        use DataType as T;
        let table = [
            // Integers into floats, by significand width.
            (T::Int32, T::Float64, Exact),
            (T::UInt32, T::Float64, Exact),
            (T::Int64, T::Float64, Precision),
            (T::UInt64, T::Float64, Precision),
            (T::Int32, T::Float32, Precision),
            (T::Int16, T::Float32, Exact),
            (T::UInt8, T::Float16, Exact),
            (T::Int8, T::Float16, Exact),
            (T::Int16, T::Float16, Precision),
            (T::UInt16, T::Float16, RangeAndPrecision),
            (T::Int32, T::Float16, RangeAndPrecision),
            // Integers into integers.
            (T::Int32, T::Int64, Exact),
            (T::Int64, T::UInt64, RangeOnly),
            (T::UInt64, T::Int64, RangeOnly),
            (T::UInt8, T::Int16, Exact),
            (T::Int8, T::UInt8, RangeOnly),
            // Floats.
            (T::Float32, T::Float64, Exact),
            (T::Float16, T::Float32, Exact),
            (T::Float64, T::Float32, RangeAndPrecision),
            (T::Float32, T::Float16, RangeAndPrecision),
            (T::Float64, T::Int64, RangeAndPrecision),
            (T::Float16, T::Int64, RangeAndPrecision),
            // A float's finest bit needs -min_exponent decimal places.
            (T::Float16, T::Decimal128(29, 24), RangeOnly),
            (T::Float16, T::Decimal128(30, 23), RangeAndPrecision),
            (T::Float64, T::Decimal128(30, 15), RangeAndPrecision),
            (T::Float64, T::Decimal256(76, 20), RangeAndPrecision),
            // Decimals into floats, by mathematical representability.
            (T::Decimal32(9, 0), T::Float64, Exact),
            (T::Decimal32(9, 0), T::Float32, Precision),
            (T::Decimal128(15, 0), T::Float64, Exact),
            (T::Decimal128(16, 0), T::Float64, Precision),
            (T::Decimal128(10, 2), T::Float64, Precision),
            (T::Decimal256(76, 0), T::Float16, RangeAndPrecision),
            (T::Decimal128(2, -3), T::Float64, Exact),
            (T::Decimal128(1, -3), T::Float16, Exact),
            (T::Decimal128(2, -3), T::Float16, RangeAndPrecision),
            // Integers into decimals.
            (T::Int64, T::Decimal128(20, 0), Exact),
            (T::Int64, T::Decimal128(19, 0), Exact),
            (T::Int64, T::Decimal64(18, 0), RangeOnly),
            (T::Int64, T::Decimal32(9, 0), RangeOnly),
            (T::Int64, T::Decimal128(38, 33), RangeOnly),
            (T::Int64, T::Decimal256(76, 20), Exact),
            (T::Int64, T::Decimal64(18, -1), Precision),
            (T::UInt64, T::Decimal128(22, 2), Exact),
            // Decimals into integers: an Int64 holds every 18-digit integer,
            // not every 19-digit one.
            (T::Decimal32(9, 2), T::Int64, Precision),
            (T::Decimal64(18, 0), T::Int64, Exact),
            (T::Decimal64(17, -1), T::Int64, Exact),
            (T::Decimal64(18, -1), T::Int64, RangeOnly),
            (T::Decimal32(9, -10), T::Int64, RangeOnly),
            (T::Decimal128(19, 0), T::Int64, RangeOnly),
            (T::Decimal128(19, 0), T::UInt64, RangeOnly),
            // Arrow scales a negative-scale Decimal32 up in an i32.
            (T::Decimal32(8, -1), T::Int64, Exact),
            (T::Decimal32(9, -1), T::Int64, RangeOnly),
            // Decimals into decimals: range when the integer digits shrink,
            // precision when the scale does.
            (T::Decimal128(5, 2), T::Decimal128(36, 33), Exact),
            (T::Decimal128(5, 2), T::Decimal128(38, 33), Exact),
            (T::Decimal128(38, 10), T::Decimal128(38, 20), RangeOnly),
            (T::Decimal128(38, -1), T::Decimal128(38, 38), RangeOnly),
            (T::Decimal128(14, 7), T::Decimal128(38, 33), RangeOnly),
            (T::Decimal128(38, 20), T::Decimal128(38, 10), Precision),
            (
                T::Decimal128(38, 20),
                T::Decimal128(20, 10),
                RangeAndPrecision,
            ),
            (T::Decimal128(37, -1), T::Decimal128(38, 0), Exact),
            (T::Decimal32(9, 2), T::Decimal64(18, 2), Exact),
            (T::Decimal64(18, 2), T::Decimal128(22, 2), Exact),
            (T::Decimal128(38, 10), T::Decimal256(76, 10), Exact),
            (T::Decimal256(76, 0), T::Decimal128(38, 0), RangeOnly),
            // A dictionary casts as its value type.
            (
                T::Dictionary(Box::new(T::Int8), Box::new(T::Int64)),
                T::Int64,
                Exact,
            ),
            (
                T::Dictionary(Box::new(T::Int8), Box::new(T::Int64)),
                T::Float64,
                Precision,
            ),
        ];
        for (from, to, expected) in table {
            assert_eq!(cast_class(&from, &to), expected, "{from} -> {to}");
        }
    }

    /// Dates and timestamps: range at the coarser unit's reach, precision at
    /// the finer unit's resolution; a naive and a zoned timestamp, and two
    /// zones, are of different kinds.
    #[test]
    fn cast_classes_between_instants() {
        use CastClass::{Exact, Kind, Precision, RangeAndPrecision, RangeOnly};
        use DataType as T;
        let ts = |unit| T::Timestamp(unit, None);
        let table = [
            (T::Date32, T::Date64, Exact),
            (T::Date64, T::Date32, RangeAndPrecision),
            (T::Date64, ts(Millisecond), Exact),
            (ts(Millisecond), T::Date64, Exact),
            (ts(Microsecond), T::Date64, Precision),
            (ts(Second), T::Date64, RangeOnly),
            (ts(Second), ts(Microsecond), RangeOnly),
            (ts(Microsecond), ts(Nanosecond), RangeOnly),
            (ts(Nanosecond), ts(Microsecond), Precision),
            (ts(Nanosecond), ts(Second), Precision),
            (T::Date32, ts(Nanosecond), RangeOnly),
            (T::Date32, ts(Second), Exact),
            (ts(Second), T::Date32, RangeAndPrecision),
            (
                ts(Microsecond),
                T::Timestamp(Microsecond, Some("UTC".into())),
                Kind,
            ),
            (
                T::Timestamp(Microsecond, Some("UTC".into())),
                ts(Microsecond),
                Kind,
            ),
            (
                T::Timestamp(Microsecond, Some("UTC".into())),
                T::Timestamp(Microsecond, Some("+09:00".into())),
                Kind,
            ),
            (
                T::Timestamp(Microsecond, Some("+09:00".into())),
                T::Timestamp(Nanosecond, Some("UTC".into())),
                Kind,
            ),
            (
                T::Timestamp(Second, Some("UTC".into())),
                T::Timestamp(Nanosecond, Some("UTC".into())),
                RangeOnly,
            ),
            (T::Date32, T::Timestamp(Second, Some("UTC".into())), Kind),
        ];
        for (from, to, expected) in table {
            assert_eq!(cast_class(&from, &to), expected, "{from} -> {to}");
        }
    }

    /// Changing what a value is (a number read as text, a day count read as
    /// a date) is a `Kind` cast; pairs of unjudged kinds are `Unjudged`;
    /// nothing outside this module's arithmetic is ever `Exact`.
    #[test]
    fn cast_classes_between_kinds() {
        use CastClass::{Exact, Kind, Unjudged};
        use DataType as T;
        let table = [
            (T::Int64, T::Utf8, Kind),
            (T::Utf8, T::Int64, Kind),
            (T::Float64, T::Utf8, Kind),
            (T::Boolean, T::Int64, Kind),
            (T::Int64, T::Boolean, Kind),
            (T::Int64, T::Date32, Kind),
            (T::Date32, T::Int64, Kind),
            (T::Int64, T::Timestamp(Microsecond, None), Kind),
            (T::Duration(Second), T::Timestamp(Second, None), Kind),
            (T::Time64(Microsecond), T::Int64, Kind),
            (T::Int64, T::Null, Kind),
            (T::Null, T::Int64, Exact),
            (T::Null, T::Utf8, Exact),
            (T::Null, T::Null, Exact),
            (T::Utf8, T::Utf8, Exact),
            (T::Utf8, T::Utf8View, Unjudged),
            (T::Utf8, T::LargeUtf8, Unjudged),
            (T::Boolean, T::Utf8, Unjudged),
            (T::Binary, T::LargeBinary, Unjudged),
            (
                T::List(Arc::new(Field::new("item", T::Int64, true))),
                T::Int64,
                Kind,
            ),
        ];
        for (from, to, expected) in table {
            assert_eq!(cast_class(&from, &to), expected, "{from} -> {to}");
        }
    }

    /// A float literal is the binary value its bits encode: 0.1 is
    /// 0.1000000000000000055511151231257827…, held exactly by a decimal of
    /// 55 places and placed below 0.1 by one of fewer.
    #[test]
    fn floats_held_by_decimals() {
        use DataType as T;
        use ScalarValue as S;
        let f = |v| S::Float64(Some(v));
        let table = [
            (
                f(2.5),
                T::Decimal128(38, 33),
                exactly(dec(25 * 10i128.pow(32), 38, 33)),
            ),
            (
                f(2.5),
                T::Decimal256(76, 33),
                exactly(dec256(i256::from(25) * ten(32), 76, 33)),
            ),
            (
                f(2.5),
                T::Decimal128(30, 15),
                exactly(dec(25 * 10i128.pow(14), 30, 15)),
            ),
            (
                f(0.1),
                T::Decimal128(30, 15),
                between(dec(10i128.pow(14), 30, 15)),
            ),
            (
                f(0.1),
                T::Decimal128(38, 20),
                between(dec(10_000_000_000_000_000_555, 38, 20)),
            ),
            (
                f(0.1),
                T::Decimal256(76, 55),
                exactly(big_decimal(
                    "1000000000000000055511151231257827021181583404541015625",
                    76,
                    55,
                )),
            ),
            (
                f(-0.1),
                T::Decimal128(30, 15),
                between(dec(-(10i128.pow(14)) - 1, 30, 15)),
            ),
            (f(1.1), T::Decimal128(5, 2), between(dec(110, 5, 2))),
            (f(1.236), T::Decimal32(9, 2), between(dec32(123, 9, 2))),
            (f(1.236), T::Decimal64(18, 2), between(dec64(123, 18, 2))),
            (
                f(1.236),
                T::Decimal256(76, 20),
                between(dec256(i256::from(123_599_999_999_999_998_756_i128), 76, 20)),
            ),
            (f(1.5), T::Decimal32(9, 2), exactly(dec32(150, 9, 2))),
            (f(1.5), T::Decimal64(18, 2), exactly(dec64(150, 18, 2))),
            (
                f(1.5),
                T::Decimal256(20, 2),
                exactly(dec256(i256::from(150), 20, 2)),
            ),
            (
                f(1.5),
                T::Decimal256(76, 20),
                exactly(dec256(i256::from(15) * ten(19), 76, 20)),
            ),
            (
                S::Float32(Some(1.5)),
                T::Decimal64(18, 2),
                exactly(dec64(150, 18, 2)),
            ),
            (half(1.5), T::Decimal32(9, 1), exactly(dec32(15, 9, 1))),
            (f(1e8), T::Decimal32(9, 2), beyond(Greater)),
            (f(-1e8), T::Decimal32(9, 2), beyond(Less)),
            // An integral float past i128 is its exact integer.
            (
                f(2f64.powi(200)),
                T::Decimal256(76, 0),
                exactly(dec256(i256::ONE << 200u8, 76, 0)),
            ),
            (f(1e300), T::Decimal256(76, 0), beyond(Greater)),
            // 1e-300 is within one step of zero at every decimal scale.
            (
                f(1e-300),
                T::Decimal256(76, 0),
                between(dec256(i256::ZERO, 76, 0)),
            ),
            (
                f(-1e-300),
                T::Decimal256(76, 0),
                between(dec256(i256::MINUS_ONE, 76, 0)),
            ),
            (f(0.0), T::Decimal128(5, 2), exactly(dec(0, 5, 2))),
            (f(-0.0), T::Decimal128(5, 2), exactly(dec(0, 5, 2))),
            (
                f(f64::NAN),
                T::Decimal256(76, 0),
                Holding::Not(Placement::NotANumber),
            ),
            (f(f64::INFINITY), T::Decimal128(5, 2), beyond(Greater)),
            (f(f64::NEG_INFINITY), T::Decimal128(5, 2), beyond(Less)),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    #[test]
    fn floats_held_by_integers() {
        use DataType as T;
        use ScalarValue as S;
        let f = |v| S::Float64(Some(v));
        let table = [
            (
                f(2f64.powi(63)),
                T::UInt64,
                exactly(S::UInt64(Some(1 << 63))),
            ),
            (f(1.5), T::Int64, between(S::Int64(Some(1)))),
            (f(-1.5), T::Int64, between(S::Int64(Some(-2)))),
            (f(2.0), T::Int64, exactly(S::Int64(Some(2)))),
            (f(-0.0), T::Int64, exactly(S::Int64(Some(0)))),
            (f(f64::NAN), T::Int64, Holding::Not(Placement::NotANumber)),
            (f(f64::INFINITY), T::Int64, beyond(Greater)),
            (f(f64::NEG_INFINITY), T::Int64, beyond(Less)),
            // The literal 2^53 + 1 is not an f64: it reads as 2^53.
            (
                f(9_007_199_254_740_993.0),
                T::Int64,
                exactly(S::Int64(Some(9_007_199_254_740_992))),
            ),
            (f(1e19), T::Int64, beyond(Greater)),
            (
                f(1e19),
                T::UInt64,
                exactly(S::UInt64(Some(10_000_000_000_000_000_000))),
            ),
            (f(-1.0), T::UInt64, beyond(Less)),
            (f(2f64.powi(31)), T::Int32, beyond(Greater)),
            (
                f(2f64.powi(31) - 1.0),
                T::Int32,
                exactly(S::Int32(Some(i32::MAX))),
            ),
            (half(1.5), T::Int8, between(S::Int8(Some(1)))),
            (
                S::Float32(Some(-128.0)),
                T::Int8,
                exactly(S::Int8(Some(-128))),
            ),
            // Past the greatest value, not between it and the next.
            (half(127.5), T::Int8, beyond(Greater)),
            (half(-128.5), T::Int8, beyond(Less)),
            (f(255.5), T::UInt8, beyond(Greater)),
            (f(-0.5), T::UInt8, beyond(Less)),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    /// Between float formats: 0.1 as an f32 is above 0.1 as an f64, so the
    /// f64 sits between two f32 values; a value below the smallest subnormal
    /// sits between zero and it.
    #[test]
    fn floats_held_by_floats() {
        use DataType as T;
        use ScalarValue as S;
        let f = |v| S::Float64(Some(v));
        let single = |v| S::Float32(Some(v));
        let table = [
            (f(0.1), T::Float32, between(single(0.1f32.next_down()))),
            (f(-0.1), T::Float32, between(single(-0.1f32))),
            (f(1.1), T::Float32, between(single(1.1f32.next_down()))),
            (f(-1.1), T::Float32, between(single(-1.1f32))),
            (single(0.1), T::Float64, exactly(f(f64::from(0.1f32)))),
            (f(1e39), T::Float32, beyond(Greater)),
            (f(-1e39), T::Float32, beyond(Less)),
            (f(f32::MAX.into()), T::Float32, exactly(single(f32::MAX))),
            (f(-0.0), T::Float32, exactly(single(-0.0))),
            (f(0.0), T::Float16, exactly(half(0.0))),
            (f(1.0), T::Float16, exactly(half(1.0))),
            (f(-1.5), T::Float16, exactly(half(-1.5))),
            (f(65504.0), T::Float16, exactly(half(65504.0))),
            (f(65505.0), T::Float16, beyond(Greater)),
            (f(2049.0), T::Float16, between(half(2048.0))),
            (f(f64::NAN), T::Float32, exactly(single(f32::NAN))),
            (f(f64::INFINITY), T::Float16, exactly(half(f64::INFINITY))),
            (
                f(f64::NEG_INFINITY),
                T::Float32,
                exactly(single(f32::NEG_INFINITY)),
            ),
            (
                f(2f64.powi(-149)),
                T::Float32,
                exactly(single(f32::from_bits(1))),
            ),
            (f(2f64.powi(-150)), T::Float32, between(single(0.0))),
            (
                f(-(2f64.powi(-150))),
                T::Float32,
                between(single(-f32::from_bits(1))),
            ),
            (
                f(3.0 * 2f64.powi(-150)),
                T::Float32,
                between(single(f32::from_bits(1))),
            ),
            (
                f(2f64.powi(-24)),
                T::Float16,
                exactly(S::Float16(Some(Half::from_bits(1)))),
            ),
            (f(2f64.powi(-25)), T::Float16, between(half(0.0))),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
        // Every value of a narrower format is a value of a wider one, and
        // comes back from the wider one unchanged.
        for bits in [1u16, 0x0400, 0x3c00, 0x3555, 0x7bff, 0x8001, 0xfbff] {
            let narrow = Half::from_bits(bits);
            let wide = f64::from(narrow);
            assert_eq!(
                holds(&T::Float64, &half(wide)),
                exactly(f(wide)),
                "{bits:#x}"
            );
            assert_eq!(
                holds(&T::Float16, &f(wide)),
                exactly(half(wide)),
                "{bits:#x}"
            );
        }
        for v in [0.1f32, 1e-40, f32::MAX, f32::MIN_POSITIVE, 3.0, 1e30, -2.5] {
            let wide = f64::from(v);
            assert_eq!(holds(&T::Float64, &single(v)), exactly(f(wide)), "{v}");
            assert_eq!(holds(&T::Float32, &f(wide)), exactly(single(v)), "{v}");
        }
    }

    /// An integer or decimal at a float type, by significand width.
    #[test]
    fn numbers_held_by_floats() {
        use DataType as T;
        use ScalarValue as S;
        let f = |v| S::Float64(Some(v));
        let int = |v| S::Int64(Some(v));
        let table = [
            (
                int(1 << 53),
                T::Float64,
                exactly(f(9_007_199_254_740_992.0)),
            ),
            (
                int((1 << 53) + 1),
                T::Float64,
                between(f(9_007_199_254_740_992.0)),
            ),
            (
                int(-(1 << 53) - 1),
                T::Float64,
                between(f(-9_007_199_254_740_994.0)),
            ),
            (
                int((1 << 53) - 1),
                T::Float64,
                exactly(f(9_007_199_254_740_991.0)),
            ),
            (
                int(1 << 24),
                T::Float32,
                exactly(S::Float32(Some(16_777_216.0))),
            ),
            (
                int((1 << 24) + 1),
                T::Float32,
                between(S::Float32(Some(16_777_216.0))),
            ),
            (
                int((1 << 24) - 1),
                T::Float32,
                exactly(S::Float32(Some(16_777_215.0))),
            ),
            (
                S::UInt64(Some(u64::MAX)),
                T::Float64,
                between(f(18_446_744_073_709_549_568.0)),
            ),
            (
                int(i64::MIN),
                T::Float64,
                exactly(f(-9_223_372_036_854_775_808.0)),
            ),
            (
                int(i64::MAX),
                T::Float64,
                between(f(9_223_372_036_854_774_784.0)),
            ),
            (int(65504), T::Float16, exactly(half(65504.0))),
            (int(65505), T::Float16, beyond(Greater)),
            (int(-65505), T::Float16, beyond(Less)),
            (int(2049), T::Float16, between(half(2048.0))),
            (int(-2049), T::Float16, between(half(-2050.0))),
            (S::Int8(Some(-128)), T::Float64, exactly(f(-128.0))),
            (int(0), T::Float32, exactly(S::Float32(Some(0.0)))),
            (dec(5, 2, 1), T::Float64, exactly(f(0.5))),
            (dec(1, 2, 1), T::Float64, between(f(0.1f64.next_down()))),
            (dec(-1, 2, 1), T::Float64, between(f(-0.1))),
            (dec(0, 2, 1), T::Float64, exactly(f(0.0))),
            (
                dec(59_604_644_775_390_625, 24, 24),
                T::Float16,
                exactly(S::Float16(Some(Half::from_bits(1)))),
            ),
            (dec(100_000, 6, 0), T::Float16, beyond(Greater)),
            (
                dec(125, 3, -1),
                T::Float32,
                exactly(S::Float32(Some(1250.0))),
            ),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    #[test]
    fn numbers_held_by_integers() {
        use DataType as T;
        use ScalarValue as S;
        let table = [
            (dec(15, 2, 1), T::Int64, between(S::Int64(Some(1)))),
            (dec(-15, 2, 1), T::Int64, between(S::Int64(Some(-2)))),
            (dec(20, 2, 1), T::Int64, exactly(S::Int64(Some(2)))),
            (
                dec(i128::from(i64::MAX) + 1, 20, 0),
                T::Int64,
                beyond(Greater),
            ),
            (S::Int64(Some(-1)), T::UInt64, beyond(Less)),
            (
                dec(i128::from(u64::MAX), 20, 0),
                T::UInt64,
                exactly(S::UInt64(Some(u64::MAX))),
            ),
            (
                dec(1 << 63, 19, 0),
                T::UInt64,
                exactly(S::UInt64(Some(1 << 63))),
            ),
            (dec(1 << 63, 19, 0), T::Int64, beyond(Greater)),
            (S::Int64(Some(300)), T::Int8, beyond(Greater)),
            (S::Int64(Some(-128)), T::Int8, exactly(S::Int8(Some(-128)))),
            (dec(1275, 4, 1), T::Int8, beyond(Greater)),
            (dec(1265, 4, 1), T::Int8, between(S::Int8(Some(126)))),
            (S::Int64(Some(1 << 31)), T::Int32, beyond(Greater)),
            (S::Int64(Some(255)), T::UInt8, exactly(S::UInt8(Some(255)))),
            (S::UInt64(Some(u64::MAX)), T::Int64, beyond(Greater)),
            (dec(125, 3, -1), T::Int16, exactly(S::Int16(Some(1250)))),
            (dec(125, 3, -1), T::Int8, beyond(Greater)),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    /// The decimal table the earlier #225 branch checked against an exact
    /// `Fraction` oracle, now through `holds`, at every width.
    #[test]
    fn numbers_held_by_decimals() {
        use DataType as T;
        use ScalarValue as S;
        let table = [
            (
                dec(1236, 4, 3),
                T::Decimal128(5, 2),
                between(dec(123, 5, 2)),
            ),
            (
                dec(1230, 4, 3),
                T::Decimal128(5, 2),
                exactly(dec(123, 5, 2)),
            ),
            // Past the greatest value, not between it and the next.
            (dec(999_995, 6, 3), T::Decimal128(5, 2), beyond(Greater)),
            (dec(-999_995, 6, 3), T::Decimal128(5, 2), beyond(Less)),
            (
                dec(999_990, 6, 3),
                T::Decimal128(5, 2),
                exactly(dec(99_999, 5, 2)),
            ),
            (dec(99, 2, 0), T::Decimal128(1, -1), beyond(Greater)),
            (dec(90, 2, 0), T::Decimal128(1, -1), exactly(dec(9, 1, -1))),
            (
                dec(1_234_567_890_123_456_789, 19, 19),
                T::Decimal128(38, 10),
                between(dec(1_234_567_890, 38, 10)),
            ),
            (
                dec(-1_234_567_890_123_456_789, 19, 19),
                T::Decimal128(38, 10),
                between(dec(-1_234_567_891, 38, 10)),
            ),
            (
                dec(15 * 10i128.pow(18), 20, 19),
                T::Decimal128(38, 10),
                exactly(dec(15 * 10i128.pow(9), 38, 10)),
            ),
            (
                dec(10i128.pow(38) - 1, 38, 0),
                T::Decimal128(38, 10),
                beyond(Greater),
            ),
            (
                dec(-(10i128.pow(38) - 1), 38, 0),
                T::Decimal128(38, 10),
                beyond(Less),
            ),
            (
                dec(15 * 10i128.pow(30), 32, 31),
                T::Decimal128(20, 0),
                between(dec(1, 20, 0)),
            ),
            (
                dec(-15 * 10i128.pow(30), 32, 31),
                T::Decimal128(20, 0),
                between(dec(-2, 20, 0)),
            ),
            (
                dec(2 * 10i128.pow(30), 31, 30),
                T::Decimal128(20, 0),
                exactly(dec(2, 20, 0)),
            ),
            // A negative-scale operand 39 or more digits coarser than the
            // value: the value is within one step of zero.
            (
                dec(1, 1, 38),
                T::Decimal128(38, -1),
                between(dec(0, 38, -1)),
            ),
            (
                dec(-1, 1, 38),
                T::Decimal128(38, -1),
                between(dec(-1, 38, -1)),
            ),
            (
                dec(0, 1, 38),
                T::Decimal128(38, -1),
                exactly(dec(0, 38, -1)),
            ),
            (
                dec(10i128.pow(38) - 1, 38, 38),
                T::Decimal128(38, -38),
                between(dec(0, 38, -38)),
            ),
            (
                dec(15, 2, 0),
                T::Decimal128(38, -1),
                between(dec(1, 38, -1)),
            ),
            (
                dec(10i128.pow(18), 19, 0),
                T::Decimal128(38, 20),
                beyond(Greater),
            ),
            (
                dec(15, 2, 1),
                T::Decimal128(10, -1),
                between(dec(0, 10, -1)),
            ),
            (
                dec(20, 2, 0),
                T::Decimal128(10, -1),
                exactly(dec(2, 10, -1)),
            ),
            // Every width: each bound is a value of the operand's own width,
            // and a Decimal256 holds values past every i128.
            (
                dec(1236, 4, 3),
                T::Decimal32(9, 2),
                between(dec32(123, 9, 2)),
            ),
            (dec(15, 2, 1), T::Decimal32(9, 2), exactly(dec32(150, 9, 2))),
            (
                dec(10i128.pow(7), 8, 0),
                T::Decimal32(9, 2),
                beyond(Greater),
            ),
            (
                dec(15, 2, 1),
                T::Decimal64(18, -2),
                between(dec64(0, 18, -2)),
            ),
            (
                dec(-15, 2, 1),
                T::Decimal64(18, -2),
                between(dec64(-1, 18, -2)),
            ),
            (
                dec(300, 3, 0),
                T::Decimal64(18, -2),
                exactly(dec64(3, 18, -2)),
            ),
            (
                dec(15, 2, 1),
                T::Decimal256(76, 20),
                exactly(dec256(i256::from(15) * ten(19), 76, 20)),
            ),
            // 10^37 at scale 20 is 10^57 unscaled: past i128, within 76 digits.
            (
                dec(10i128.pow(37), 38, 0),
                T::Decimal256(76, 20),
                exactly(dec256(ten(57), 76, 20)),
            ),
            (
                dec256(ten(56), 76, 0),
                T::Decimal256(76, 20),
                beyond(Greater),
            ),
            (dec256(-ten(56), 76, 0), T::Decimal256(76, 20), beyond(Less)),
            (
                dec256(i256::ONE, 76, 76),
                T::Decimal256(76, 0),
                between(dec256(i256::ZERO, 76, 0)),
            ),
            (
                dec256(i256::MINUS_ONE, 76, 76),
                T::Decimal256(76, 0),
                between(dec256(i256::MINUS_ONE, 76, 0)),
            ),
            (
                dec256(i256::ZERO, 76, 76),
                T::Decimal256(76, 0),
                exactly(dec256(i256::ZERO, 76, 0)),
            ),
            (
                dec(10i128.pow(37), 38, 0),
                T::Decimal256(76, 38),
                exactly(dec256(ten(75), 76, 38)),
            ),
            (
                dec(10i128.pow(37), 38, 0),
                T::Decimal256(76, 40),
                beyond(Greater),
            ),
            // Integers.
            (
                S::Int64(Some(1)),
                T::Decimal256(76, 70),
                exactly(dec256(ten(70), 76, 70)),
            ),
            (S::Int64(Some(1)), T::Decimal32(9, 9), beyond(Greater)),
            (S::Int64(Some(-1)), T::Decimal32(9, 9), beyond(Less)),
            (
                S::Int64(Some(1250)),
                T::Decimal32(3, -1),
                exactly(dec32(125, 3, -1)),
            ),
            (
                S::Int64(Some(1255)),
                T::Decimal32(3, -1),
                between(dec32(125, 3, -1)),
            ),
            (
                S::UInt64(Some(u64::MAX)),
                T::Decimal128(22, 2),
                exactly(dec(i128::from(u64::MAX) * 100, 22, 2)),
            ),
            (
                S::Int8(Some(-5)),
                T::Decimal64(18, 2),
                exactly(dec64(-500, 18, 2)),
            ),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    /// A timestamp is its instant (or wall-clock time, when both it and the
    /// type are naive); a date is its UTC midnight; a Date64 holds only
    /// midnights.
    #[test]
    fn instants_held_by_instants() {
        use DataType as T;
        use ScalarValue as S;
        let ts = |unit, v| timestamp_scalar(unit, Some(v), None);
        let table = [
            (
                ts(Microsecond, 1_500_000),
                T::Timestamp(Second, None),
                between(ts(Second, 1)),
            ),
            (
                ts(Microsecond, -1_500_000),
                T::Timestamp(Second, None),
                between(ts(Second, -2)),
            ),
            (
                ts(Second, 2),
                T::Timestamp(Microsecond, None),
                exactly(ts(Microsecond, 2_000_000)),
            ),
            // Year 2300 is past the nanosecond range.
            (
                ts(Microsecond, 10_413_792_000_000_000),
                T::Timestamp(Nanosecond, None),
                beyond(Greater),
            ),
            (
                ts(Second, i64::MAX),
                T::Timestamp(Nanosecond, None),
                beyond(Greater),
            ),
            (
                ts(Second, i64::MIN),
                T::Timestamp(Nanosecond, None),
                beyond(Less),
            ),
            (
                ts(Nanosecond, i64::MAX),
                T::Timestamp(Microsecond, None),
                between(ts(Microsecond, i64::MAX / 1_000)),
            ),
            (
                S::Date32(Some(1)),
                T::Timestamp(Microsecond, None),
                exactly(ts(Microsecond, 86_400_000_000)),
            ),
            // 06:00 on day 19723 is between that date and the next.
            (
                ts(Second, 19_723 * 86_400 + 6 * 3_600),
                T::Date32,
                between(S::Date32(Some(19_723))),
            ),
            (
                ts(Second, 19_723 * 86_400),
                T::Date32,
                exactly(S::Date32(Some(19_723))),
            ),
            (ts(Second, -1), T::Date32, between(S::Date32(Some(-1)))),
            (
                ts(Microsecond, 86_400_000_000),
                T::Date32,
                exactly(S::Date32(Some(1))),
            ),
            (
                ts(Microsecond, 86_400_000_001),
                T::Date32,
                between(S::Date32(Some(1))),
            ),
            (
                ts(Second, 86_400),
                T::Date64,
                exactly(S::Date64(Some(86_400_000))),
            ),
            (
                ts(Second, 86_400 + 6 * 3_600),
                T::Date64,
                between(S::Date64(Some(86_400_000))),
            ),
            (
                ts(Millisecond, -1),
                T::Date64,
                between(S::Date64(Some(-86_400_000))),
            ),
            (
                S::Date32(Some(19_723)),
                T::Date64,
                exactly(S::Date64(Some(19_723 * 86_400_000))),
            ),
            (
                S::Date64(Some(86_400_000)),
                T::Date32,
                exactly(S::Date32(Some(1))),
            ),
            (S::Date64(Some(1)), T::Date32, between(S::Date32(Some(0)))),
            (S::Date64(Some(i64::MAX)), T::Date32, beyond(Greater)),
            // An aware timestamp is its instant: midnight in UTC+9 is 15:00
            // UTC the day before.
            (
                zoned_ts(Second, 19_723 * 86_400 - 9 * 3_600, "+09:00"),
                T::Date32,
                between(S::Date32(Some(19_722))),
            ),
            (
                zoned_ts(Second, 19_723 * 86_400, "+09:00"),
                T::Date32,
                exactly(S::Date32(Some(19_723))),
            ),
            (
                zoned_ts(Second, 1, "UTC"),
                T::Timestamp(Microsecond, Some("+09:00".into())),
                exactly(zoned_ts(Microsecond, 1_000_000, "+09:00")),
            ),
            (
                zoned_ts(Microsecond, 5, "UTC"),
                T::Timestamp(Microsecond, Some("UTC".into())),
                exactly(zoned_ts(Microsecond, 5, "UTC")),
            ),
            // Reading a naive time in a zone, or a zoned instant as a naive
            // time, is not a fact about the value.
            (
                zoned_ts(Second, 1, "UTC"),
                T::Timestamp(Microsecond, None),
                Holding::Unjudged,
            ),
            (
                ts(Microsecond, 5),
                T::Timestamp(Microsecond, Some("UTC".into())),
                Holding::Unjudged,
            ),
            (
                S::Date32(Some(1)),
                T::Timestamp(Microsecond, Some("UTC".into())),
                Holding::Unjudged,
            ),
        ];
        for (value, to, expected) in table {
            assert_eq!(holds(&to, &value), expected, "{value:?} at {to}");
        }
    }

    /// Kinds this module has no arithmetic for are not judged, except that
    /// every type holds its own values and the typed null.
    #[test]
    fn other_kinds_are_not_held() {
        use DataType as T;
        use ScalarValue as S;
        let text = |s: &str| S::Utf8(Some(s.to_string()));
        let unjudged = [
            (text("1.5"), T::Decimal128(5, 2)),
            (text("05"), T::Int64),
            (text("abc"), T::Int64),
            (text("2024-01-01"), T::Date32),
            (text("x"), T::Utf8View),
            (S::Boolean(Some(true)), T::Int64),
            (S::Int64(Some(1)), T::Boolean),
            (S::Int64(Some(0)), T::Utf8),
            (S::Float64(Some(1.5)), T::Utf8),
            (S::Int64(Some(5)), T::Date32),
            (S::Int64(Some(5)), T::Timestamp(Microsecond, None)),
            (S::Date32(Some(1)), T::Int64),
            (S::Date32(Some(1)), T::Float64),
            (timestamp_scalar(Second, Some(1), None), T::Utf8),
            (S::Int64(Some(1)), T::Duration(Second)),
            (S::DurationSecond(Some(1)), T::Int64),
        ];
        for (value, to) in unjudged {
            assert_eq!(holds(&to, &value), Holding::Unjudged, "{value:?} at {to}");
        }
        let held = [
            (text("x"), T::Utf8, text("x")),
            (S::Boolean(Some(true)), T::Boolean, S::Boolean(Some(true))),
            (
                S::Int64(None),
                T::Decimal128(5, 2),
                S::Decimal128(None, 5, 2),
            ),
            (S::Null, T::Int64, S::Int64(None)),
            (S::Utf8(None), T::Int64, S::Int64(None)),
            (S::Int64(None), T::Utf8, S::Utf8(None)),
            (
                S::Null,
                T::Timestamp(Microsecond, Some("UTC".into())),
                timestamp_scalar(Microsecond, None, Some("UTC".into())),
            ),
            (
                S::Int64(Some(5)),
                T::Dictionary(Box::new(T::Int8), Box::new(T::Int64)),
                S::Int64(Some(5)),
            ),
            (
                S::Dictionary(Box::new(T::Int8), Box::new(S::Int64(Some(5)))),
                T::Int64,
                S::Int64(Some(5)),
            ),
            (
                S::Dictionary(Box::new(T::Int8), Box::new(S::Int64(Some(5)))),
                T::Float32,
                S::Float32(Some(5.0)),
            ),
        ];
        for (value, to, expected) in held {
            assert_eq!(holds(&to, &value), exactly(expected), "{value:?} at {to}");
        }
    }

    #[test]
    fn rational_arithmetic() {
        let r = |n: i64, d: i64| Rational {
            num: BigInt::from(n),
            den: BigInt::from(d),
        };
        assert_eq!(r(7, 2).floor(), (BigInt::from(3), false));
        assert_eq!(r(-7, 2).floor(), (BigInt::from(-4), false));
        assert_eq!(r(4, 2).floor(), (BigInt::from(2), true));
        assert_eq!(r(-4, 2).floor(), (BigInt::from(-2), true));
        assert_eq!(r(1, 1).floor_log2(), 0);
        assert_eq!(r(3, 2).floor_log2(), 0);
        assert_eq!(r(2, 1).floor_log2(), 1);
        assert_eq!(r(1, 2).floor_log2(), -1);
        assert_eq!(r(1, 3).floor_log2(), -2);
        assert_eq!(r(7, 8).floor_log2(), -1);
        assert_eq!(r(1023, 1).floor_log2(), 9);
        assert_eq!(r(1024, 1).floor_log2(), 10);
        assert_eq!(binary_value(0.1).floor_log2(), -4);
        assert!(r(1, 3) < r(1, 2));
        assert!(r(-1, 3) > r(-1, 2));
        assert_eq!(r(1, 3).times_ten_to(1), r(10, 3));
        assert_eq!(r(1, 3).times_ten_to(-1), r(1, 30));
        assert_eq!(r(3, 1).times_two_to(-2).floor(), (BigInt::zero(), false));
        // A float's bits, decoded and re-encoded.
        for v in [
            0.1,
            1.236,
            1e300,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            2f64.powi(-1074) * 3.0,
        ] {
            let (mantissa, exponent, exact) = FloatFormat::Double.floor(&binary_value(v));
            assert!(exact, "{v}");
            assert_eq!(binary(mantissa, exponent), v, "{v}");
        }
        assert_eq!(binary_value(-0.0), Rational::integer(BigInt::zero()));
        assert_eq!(binary_value(-2.5), r(-5, 2));
        // (2 - 2^-52) * 2^1023 for binary64, (2 - 2^-23) * 2^127 for binary32,
        // (2 - 2^-10) * 2^15 for binary16.
        assert_eq!(
            FloatFormat::Double.max_finite(),
            (BigInt::one() << 1024) - (BigInt::one() << 971)
        );
        assert_eq!(
            FloatFormat::Single.max_finite(),
            (BigInt::one() << 128) - (BigInt::one() << 104)
        );
        assert_eq!(FloatFormat::Half.max_finite(), BigInt::from(65504));
    }
}
