"""Each literal rule next to the same type pair with two columns.

A rule that patches a DataFusion defect for literals only shows up
here as a difference between the ``_literal`` and ``_column`` cases.
Design review Part V, "Column pairs" (#225).
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq, coalesce, if_else
from ltseq.expr import LiteralExpr


def units():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 2],
        "s": pa.array([1, 1, None], pa.timestamp("s")),
        "us": pa.array([1000001, 1000000, 5], pa.timestamp("us")),
        "lit_us": pa.array([1000001, 1000001, 1000001], pa.timestamp("us")),
    }))


FINE_US = datetime(1970, 1, 1, 0, 0, 1, 1)


# Timestamp units
def case_unit_literal_eq(): return units().derive(v=lambda r: r.s == FINE_US).select("v")
def case_unit_column_eq(): return units().derive(v=lambda r: r.s == r.lit_us).select("v")
def case_unit_columns_eq(): return units().derive(v=lambda r: r.s == r.us).select("v")
def case_unit_columns_lt(): return units().derive(v=lambda r: r.s < r.us).select("v")
def case_unit_columns_subtract(): return units().derive(v=lambda r: r.us - r.s).select("v")
def case_unit_columns_case(): return units().derive(v=lambda r: if_else(r.k > 5, r.s, r.us)).select("v")
def case_unit_columns_coalesce(): return units().derive(v=lambda r: coalesce(r.s, r.us)).select("v")


def case_unit_folded_literal_eq():
    # A constant CASE that DataFusion's simplifier folds to the same literal.
    return units().derive(v=lambda r: r.s == if_else(LiteralExpr(True), FINE_US, FINE_US)).select("v")


# Float and decimal
def floats():
    return LTSeq.from_arrow(pa.table({
        "f": pa.array([float("nan"), 2.0, 1e-16, None], pa.float64()),
        "p": pa.array([Decimal("1.50"), Decimal("1.50"), Decimal("0.00"), Decimal("1.00")], pa.decimal128(5, 2)),
        "zero": pa.array([Decimal("0")] * 4, pa.decimal128(1, 0)),
    }))


def case_float_gt_decimal_literal(): return floats().derive(v=lambda r: r.f > Decimal("0")).select("v")
def case_float_gt_decimal_column(): return floats().derive(v=lambda r: r.f > r.zero).select("v")
def case_float_plus_decimal_literal(): return floats().derive(v=lambda r: r.f + Decimal("0")).select("v")
def case_float_plus_decimal_column(): return floats().derive(v=lambda r: r.f + r.zero).select("v")
def case_float_fill_decimal_literal(): return floats().derive(v=lambda r: r.f.fill_null(Decimal("1"))).select("v")
def case_float_fill_decimal_column(): return floats().derive(v=lambda r: r.f.fill_null(r.zero)).select("v")
def case_decimal_gt_float_literal(): return floats().derive(v=lambda r: r.p > 1e-16).select("v")
def case_float_gt_decimal_expression(): return floats().derive(v=lambda r: r.f > (LiteralExpr(Decimal("1.5")) + 0)).select("v")


def zero_decimal():
    return LTSeq.from_arrow(pa.table({"p": pa.array([Decimal("0.00"), None], pa.decimal128(5, 2))}))


def case_decimal_gt_negative_tiny_float(): return zero_decimal().derive(v=lambda r: r.p > -1e-16).select("v")
def case_decimal_eq_tiny_float(): return zero_decimal().derive(v=lambda r: r.p == 1e-16).select("v")
def case_decimal_ne_tiny_float(): return zero_decimal().derive(v=lambda r: r.p != 1e-16).select("v")


# Zones
def zones():
    return LTSeq.from_arrow(pa.table({
        "ny": pa.array([datetime(2024, 1, 1, 5, tzinfo=timezone.utc)], pa.timestamp("us", "America/New_York")),
        "naive": pa.array([datetime(2024, 1, 1, 0)], pa.timestamp("us")),
        "d": pa.array([date(2024, 1, 1)], pa.date32()),
    }))


def case_zoned_eq_naive_literal(): return zones().derive(v=lambda r: r.ny == datetime(2024, 1, 1, 0)).select("v")
def case_zoned_eq_naive_column(): return zones().derive(v=lambda r: r.ny == r.naive).select("v")
def case_zoned_eq_date_literal(): return zones().derive(v=lambda r: r.ny == date(2024, 1, 1)).select("v")
def case_zoned_eq_date_column(): return zones().derive(v=lambda r: r.ny == r.d).select("v")
def case_naive_eq_aware_literal(): return zones().derive(v=lambda r: r.naive == datetime(2024, 1, 1, 5, tzinfo=timezone.utc)).select("v")
def case_naive_eq_zoned_column(): return zones().derive(v=lambda r: r.naive == r.ny).select("v")


# Dates against timestamps outside the nanosecond range
def far():
    return LTSeq.from_arrow(pa.table({
        "d": pa.array([date(2300, 1, 1), date(2024, 1, 1)], pa.date32()),
        "t": pa.array([datetime(2300, 1, 1), datetime(2024, 1, 1)], pa.timestamp("us")),
    }))


def case_far_date_eq_literal(): return far().derive(v=lambda r: r.d == datetime(2300, 1, 1)).select("v")
def case_far_date_eq_column(): return far().derive(v=lambda r: r.d == r.t).select("v")


# Numbers and strings against dates
def mixed():
    return LTSeq.from_arrow(pa.table({
        "i": pa.array([19723, 1], pa.int64()),
        "d": pa.array([date(2024, 1, 1), date(1970, 1, 2)], pa.date32()),
        "s": pa.array(["2024-01-01", "x"], pa.string()),
    }))


def case_integer_eq_date_literal(): return mixed().derive(v=lambda r: r.i == date(2024, 1, 1)).select("v")
def case_integer_eq_date_column(): return mixed().derive(v=lambda r: r.i == r.d).select("v")
def case_string_eq_date_literal(): return mixed().derive(v=lambda r: r.s == date(2024, 1, 1)).select("v")
def case_string_eq_date_column(): return mixed().derive(v=lambda r: r.s == r.d).select("v")


# Decimal widening past 38 digits
def wide():
    return LTSeq.from_arrow(pa.table({
        "p": pa.array([Decimal("1.23"), Decimal("999.99")], pa.decimal128(5, 2)),
        "fine": pa.array([Decimal("1.230000000000000000000000000000000")] * 2, pa.decimal128(34, 33)),
        "w": pa.array([Decimal("1.5"), Decimal("5")], pa.decimal128(38, 20)),
        "big": pa.array([10**18, 10**18], pa.int64()),
    }))


def case_fine_decimal_literal(): return wide().derive(v=lambda r: r.p == Decimal("1.230000000000000000000000000000000")).select("v")
def case_fine_decimal_column(): return wide().derive(v=lambda r: r.p == r.fine).select("v")
def case_big_integer_literal(): return wide().derive(v=lambda r: r.w == 10**18).select("v")
def case_big_integer_column(): return wide().derive(v=lambda r: r.w == r.big).select("v")
def case_big_integer_is_in(): return wide().derive(v=lambda r: r.w.is_in([5, 10**18])).select("v")
def case_big_integer_fill_null(): return wide().derive(v=lambda r: r.w.fill_null(10**18)).select("v")


# is_in with items of different kinds next to values a common float type
# merges: 2^53 and 2^53 + 1 are one Float64 (review F1 on #225)
def neighbors():
    return LTSeq.from_arrow(pa.table({"x": pa.array([2**53, 2**53 + 1, 0, None], pa.int64())}))


def case_mixed_is_in(): return neighbors().derive(v=lambda r: r.x.is_in([Decimal(2**53 + 1), 0.5])).select("v")
def case_mixed_disjunction(): return neighbors().derive(v=lambda r: (r.x == Decimal(2**53 + 1)) | (r.x == 0.5)).select("v")


# A dictionary-encoded column against the same values decoded (review F2)
SENTINEL = pa.array([Decimal("1.1234567890"), Decimal("1E27"), None], pa.decimal128(38, 10))


def decoded(): return LTSeq.from_arrow(pa.table({"x": SENTINEL}))
def encoded(): return LTSeq.from_arrow(pa.table({"x": SENTINEL.dictionary_encode()}))


def case_decoded_decimal_lt(): return decoded().derive(v=lambda r: r.x < Decimal("1.12345678905")).select("v")
def case_encoded_decimal_lt(): return encoded().derive(v=lambda r: r.x < Decimal("1.12345678905")).select("v")
