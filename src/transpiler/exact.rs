//! Exact arithmetic on literal values: moving a value between the units or
//! scales of two types without losing anything, or saying it cannot be done.

use datafusion::arrow::datatypes::TimeUnit;

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
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};

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
