"""Literals of every kind in value positions, and NULL and float literals in
the counting kernel (review of b6cc39f on #225).

`fill_null`, `coalesce`, `if_else` and `shift(default=)` judged strings,
Booleans and cross-kind values by an Arrow cast round trip: `"1.5"` was
refused for a decimal(5, 2) column, a datetime with a time of day was
stored in a Date64 column, and an aware datetime filled into a date column
was its local date. Strings keep DataFusion's reading (decision D-h).
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq, coalesce, if_else

TOKYO = timezone(timedelta(hours=9))


def table():
    return LTSeq.from_arrow(pa.table({
        "k": pa.array([0, 1], pa.int64()),
        "p": pa.array([Decimal("1.00"), None], pa.decimal128(5, 2)),
        "x": pa.array([7, None], pa.int64()),
        "s": pa.array(["a", None]),
        "b": pa.array([True, None]),
        "d": pa.array([date(2024, 1, 1), None]),
        "d64": pa.array([date(2024, 1, 1), None], pa.date64()),
        "ts": pa.array([datetime(2024, 1, 1), None], pa.timestamp("us")),
    })).sort("k")


def v(fn):
    return table().derive(v=fn).select("v")


def ms(fn):
    """A Date64 result as its milliseconds, which show a time of day."""
    return table().derive(v=fn).derive(ms=lambda r: r.v.cast("int64")).select("ms")


# String literals: DataFusion's reading, as on main.
def case_string_fill_decimal(): return v(lambda r: r.p.fill_null("1.5"))
def case_string_fill_decimal_integral(): return v(lambda r: r.p.fill_null("2"))
def case_string_fill_decimal_rounded(): return v(lambda r: r.p.fill_null("1.236"))
def case_string_coalesce_decimal(): return v(lambda r: coalesce(r.p, "1.5"))
def case_string_if_else_decimal(): return v(lambda r: if_else(r.k == 1, "1.5", r.p))
def case_string_shift_decimal(): return v(lambda r: r.p.shift(1, default="1.5"))
def case_string_fill_int_padded(): return v(lambda r: r.x.fill_null("05"))
def case_string_shift_int(): return v(lambda r: r.x.shift(1, default="5"))
def case_string_fill_int_unreadable(): return v(lambda r: r.x.fill_null("abc"))
def case_string_shift_int_unreadable(): return v(lambda r: r.x.shift(1, default="abc"))


# Booleans and numbers next to columns of another kind.
def case_bool_shift_int(): return v(lambda r: r.x.shift(1, default=True))
def case_int_shift_bool(): return v(lambda r: r.b.shift(1, default=1))
def case_int_shift_string(): return v(lambda r: r.s.shift(1, default=0))
def case_float_shift_string(): return v(lambda r: r.s.shift(1, default=1.5))
def case_int_fill_string(): return v(lambda r: r.s.fill_null(0))


# Numbers and Booleans next to date and timestamp columns.
def case_int_shift_date(): return v(lambda r: r.d.shift(1, default=5))
def case_int_shift_timestamp(): return v(lambda r: r.ts.shift(1, default=5))
def case_float_shift_timestamp(): return v(lambda r: r.ts.shift(1, default=1.5))
def case_bool_shift_date(): return v(lambda r: r.d.shift(1, default=True))
def case_int_fill_date(): return v(lambda r: r.d.fill_null(5))


# Date64 columns hold days.
def case_date64_fill_time_of_day(): return ms(lambda r: r.d64.fill_null(datetime(2024, 1, 2, 6)))
def case_date64_shift_time_of_day(): return ms(lambda r: r.d64.shift(1, default=datetime(2024, 1, 2, 6)))
def case_date64_fill_midnight(): return ms(lambda r: r.d64.fill_null(datetime(2024, 1, 2)))
def case_date64_fill_date(): return ms(lambda r: r.d64.fill_null(date(2024, 1, 2)))
def case_date64_shift_date(): return ms(lambda r: r.d64.shift(1, default=date(2024, 1, 2)))
def case_date64_gt_time_of_day(): return v(lambda r: r.d64 > datetime(2023, 12, 31, 6))


# Aware datetimes next to a date column: their UTC instant, as in comparisons.
def case_aware_fill_date_local_midnight(): return v(lambda r: r.d.fill_null(datetime(2024, 1, 2, tzinfo=TOKYO)))
def case_aware_fill_date_utc_midnight(): return v(lambda r: r.d.fill_null(datetime(2024, 1, 2, 9, tzinfo=TOKYO)))
def case_aware_shift_date_local_midnight(): return v(lambda r: r.d.shift(1, default=datetime(2024, 1, 2, tzinfo=TOKYO)))
def case_aware_eq_date_utc_midnight(): return v(lambda r: r.d == datetime(2024, 1, 1, 9, tzinfo=TOKYO))


# An integer fill next to a negative-scale Decimal32/64 column.
def negative_scale(dtype):
    largest = Decimal(10**dtype.precision - 1).scaleb(-dtype.scale)
    column = pa.array([largest, None], pa.decimal128(dtype.precision, dtype.scale)).cast(dtype)
    return LTSeq.from_arrow(pa.table({"x": column}))


def filled(dtype):
    return negative_scale(dtype).derive(v=lambda r: r.x.fill_null(0)).select("v")


def case_int_fill_decimal64_19_digits(): return filled(pa.decimal64(18, -1))
def case_int_fill_decimal32_negative_scale(): return filled(pa.decimal32(9, -1))
def case_int_fill_decimal64_18_digits(): return filled(pa.decimal64(17, -1))


# The counting kernel: a NULL literal under `&`, a float threshold outside
# the fused shape. Each is the count and the materialized reference.
def counts(pred):
    t = LTSeq.from_arrow(pa.table({"k": range(5), "x": pa.array([1, 1, 2, 3, 5], pa.int64())})).sort("k")
    return [t.group_ordered(pred).first().count(), len(t.group_ordered(pred).first().to_arrow())]


def case_count_null_comparison_and(): return counts(lambda r: (r.x != r.x.shift(1)) & (r.x > None))
def case_count_null_arithmetic_and(): return counts(lambda r: (r.x != r.x.shift(1)) & ((r.x - None) > 0))
def case_count_float_outside_fused(): return counts(lambda r: (r.x != r.x.shift(1)) | (r.x > 2.0))
def case_count_float_fused(): return counts(lambda r: (r.x != r.x.shift(1)) | ((r.x - r.x.shift(1)) > 1.0))
