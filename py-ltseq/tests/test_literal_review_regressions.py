"""Regression cases from the review rounds of #225.

Each section is one review's counterexamples, kept as the reviewer wrote
them (expected values, controls and all), so the redesigned literal
typing is held to every case the earlier implementation was. Reviews:

- c4ca298: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6000430239
- c2b465c: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6001286795
- e4217f4: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6002117054
- b22ecab: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6004458626
"""

import math
import operator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq, coalesce, if_else
from ltseq.expr import LiteralExpr


def column(table, name="v"):
    return table.to_arrow().column(name).to_pylist()


# ---------------------------------------------------------------------------
# Review of c4ca298
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dtype", [pa.date32(), pa.date64()])
@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Tokyo"])
def test_date_columns_keep_utc_midnight(dtype, zone):
    t = LTSeq.from_arrow(pa.table({
        "d": pa.array([date(2024, 1, 1), None], dtype),
        "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc), None], pa.timestamp("us", zone)),
    }))
    out = t.derive(
        eq=lambda r: r.ts == r.d,
        forward=lambda r: r.ts.dt.diff(r.d, unit="hour"),
        reverse=lambda r: r.d.dt.diff(r.ts, unit="hour"),
    ).to_arrow()
    assert out.column("eq").to_pylist() == [True, None]
    assert out.column("forward").to_pylist() == [0.0, None]
    assert out.column("reverse").to_pylist() == [0.0, None]


def test_negative_scale_does_not_panic():
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("10"), Decimal("-10"), None], pa.decimal128(38, -1)),
    }))
    assert column(t.derive(v=lambda r: r.x > Decimal("1E-38"))) == [True, False, None]


@pytest.mark.parametrize("dtype, values, default", [
    (pa.int64(), [1, 2], 1.5),
    (pa.decimal128(5, 2), [Decimal("1.00"), Decimal("2.00")], 1.236),
])
def test_lossy_float_default_rejected(dtype, values, default):
    t = LTSeq.from_arrow(pa.table({"k": [0, 1], "x": pa.array(values, dtype)})).sort("k")
    with pytest.raises(ValueError, match="cannot hold.*exactly"):
        t.derive(v=lambda r: r.x.shift(1, default=default)).to_arrow()


def test_exact_uint64_decimal_default_accepted():
    t = LTSeq.from_arrow(pa.table({"k": [0, 1], "x": pa.array([1, 2], pa.uint64())})).sort("k")
    out = t.derive(v=lambda r: r.x.shift(1, default=Decimal("9223372036854775808")))
    assert column(out) == [2**63, 1]


def test_fractional_second_offset_rejected():
    value = datetime(2024, 1, 1, tzinfo=timezone(timedelta(microseconds=500000)))
    with pytest.raises(ValueError, match="whole number of minutes"):
        LiteralExpr(value)


# ---------------------------------------------------------------------------
# Review of c2b465c
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("divisor", [Decimal("2"), Decimal("2.0")])
def test_decimal_arithmetic_count_agrees_with_materialized_groups(divisor):
    t = LTSeq.from_arrow(pa.table({"k": range(4), "x": [2, 3, 4, 5]})).sort("k")
    pred = lambda r: (r.x / divisor) > (r.x.shift(1) / divisor)  # noqa: E731
    assert column(t.derive(v=pred)) == [None, True, True, True]
    reference = t.group_ordered(pred).first().to_arrow().num_rows
    assert reference == 4
    assert t.group_ordered(pred).first().count() == reference


@pytest.mark.parametrize("zone, day", [
    ("Asia/Tokyo", date(2262, 4, 12)),
    ("America/New_York", date(1677, 9, 21)),
])
def test_local_midnight_near_nanosecond_bounds_is_not_beyond(zone, day):
    midnight = datetime.combine(day, datetime.min.time(), ZoneInfo(zone))
    delta = midnight - datetime(1970, 1, 1, tzinfo=timezone.utc)
    ticks = (delta.days * 86400 + delta.seconds) * 10**9
    assert -(2**63) <= ticks < 2**63
    array = pa.array([ticks - 1, ticks, ticks + 1, None], pa.int64()).cast(pa.timestamp("ns", zone))
    t = LTSeq.from_arrow(pa.table({"x": array}))
    result = t.derive(
        eq=lambda r: r.x == day,
        gt=lambda r: r.x > day,
        le=lambda r: r.x <= day,
        member=lambda r: r.x.is_in([day]),
        aware=lambda r: r.x == midnight,
    ).to_arrow()
    assert result.column("aware").to_pylist() == [False, True, False, None]
    assert result.column("eq").to_pylist() == [False, True, False, None]
    assert result.column("gt").to_pylist() == [False, False, True, None]
    assert result.column("le").to_pylist() == [True, True, False, None]
    assert result.column("member").to_pylist() == [False, True, False, None]


def test_nanosecond_wall_clock_literal_can_localize_into_microseconds():
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([pd.Timestamp.max.value // 1000, None], pa.timestamp("us", "America/New_York")),
    }))
    assert column(t.derive(v=lambda r: r.x < pd.Timestamp.max)) == [True, None]


def test_mixed_decimal_membership_agrees_with_equality_disjunction():
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("1.23"), Decimal("0.12"), None], pa.decimal128(5, 2)),
    }))
    big = Decimal("123456789012345678901234567890123456")
    small = Decimal("1.230000000000000000000000000000000")
    expected = [True, False, None]
    assert column(t.derive(v=lambda r: r.x.is_in([big]))) == [False, False, None]
    assert column(t.derive(v=lambda r: r.x.is_in([small]))) == expected
    assert column(t.derive(v=lambda r: (r.x == big) | (r.x == small))) == expected
    assert column(t.derive(v=lambda r: r.x.is_in([big, small]))) == expected


# ---------------------------------------------------------------------------
# Review of e4217f4
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dtype, value", [
    (pa.int64(), 2.0**63),
    (pa.uint64(), 2.0**64),
    (pa.int8(), 128.0),
])
def test_numeric_default_rejects_upper_boundary(dtype, value):
    t = LTSeq.from_arrow(pa.table({"k": [0, 1], "x": pa.array([None, None], dtype)})).sort("k")
    with pytest.raises(ValueError, match="cannot hold"):
        t.derive(v=lambda r: r.x.shift(1, default=value))


@pytest.mark.parametrize("dtype", [
    pa.decimal128(38, 10), pa.decimal128(38, -1), pa.int64(), pa.int8(), pa.uint64(),
])
@pytest.mark.parametrize("literal", [
    Decimal("-1E-38"),
    Decimal("1E-38"),
    Decimal("0E-38"),
    Decimal("-1.230000000000000000000000000000000"),
    Decimal("9" * 38),
])
def test_decimal_composition_matches_exact_oracle(dtype, literal):
    values = [-1, 0, 1, None] if pa.types.is_signed_integer(dtype) else [0, 1, 2, None]
    if pa.types.is_decimal(dtype):
        values = [Decimal("-10"), Decimal("0"), Decimal("10"), None]
    t = LTSeq.from_arrow(pa.table({"k": range(4), "x": pa.array(values, dtype)})).sort("k")
    for op in [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge]:
        expected = [None if x is None else op(Fraction(x), Fraction(literal)) for x in values]
        assert column(t.derive(v=lambda r: op(r.x, literal))) == expected
        assert column(t.derive(v=lambda r: op(r.x.shift(1), literal))) == [None] + expected[:-1]
    # A NULL list member keeps non-matches NULL even when impossible literals are dropped.
    expected = [None if x is None or Fraction(x) != Fraction(literal) else True for x in values]
    assert column(t.derive(v=lambda r: r.x.is_in([literal, None]))) == expected


def test_nary_decimal_values_include_all_column_types():
    t = LTSeq.from_arrow(pa.table({
        "p": pa.array([None, None], pa.decimal128(5, 2)),
        "pw": pa.array([None, Decimal("1000000000000000000000000000")], pa.decimal128(38, 10)),
    }))
    fine = Decimal("0.12345678900000000000")
    out = t.derive(v=lambda r: coalesce(r.p, r.pw, fine)).to_arrow().column("v")
    assert out.to_pylist() == [Decimal("0.1234567890"), Decimal("1000000000000000000000000000")]
    assert out.type == pa.decimal128(38, 10)


@pytest.mark.parametrize("op", [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge])
def test_folded_signed_zero_matches_engine(op):
    t = LTSeq.from_arrow(pa.table({"x": pa.array([-0.0], pa.float64())}))
    expected = column(t.derive(v=lambda r: op(r.x, 0.0)))
    assert column(t.derive(v=lambda r: op(LiteralExpr(-0.0), 0.0))) == expected


# ---------------------------------------------------------------------------
# Review of b22ecab
# ---------------------------------------------------------------------------

CUT = datetime(1970, 1, 1, 0, 0, 1, 500000)
FINE = Decimal("1.230000000000000000000000000000000")


def timestamps():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0, 0],
        "s": pa.array([0, 1, 0, 0], pa.timestamp("s")),
        "us": pa.array([1250000, 0, 1500000, None], pa.timestamp("us")),
    }))


def branch(r, reversed):
    return if_else(r.k <= 0, r.us, r.s) if reversed else if_else(r.k > 0, r.s, r.us)


@pytest.mark.parametrize("reversed", [False, True])
@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize("op, expected", [
    (operator.eq, [False, False, True, None]),
    (operator.lt, [True, True, False, None]),
    (operator.ge, [False, False, True, None]),
])
def test_case_timestamp_comparison(op, expected, staged, reversed):
    t = timestamps()
    actual = t.derive(c=lambda r: branch(r, reversed)).to_arrow().column("c")
    assert actual.type == pa.timestamp("us")
    assert actual.to_pylist() == [
        datetime(1970, 1, 1, 0, 0, 1, 250000), datetime(1970, 1, 1, 0, 0, 1), CUT, None,
    ]
    if staged:
        v = t.derive(c=lambda r: branch(r, reversed)).derive(v=lambda r: op(r.c, CUT))
    else:
        v = t.derive(v=lambda r: op(branch(r, reversed), CUT))
    # Control: the same comparison on a table rebuilt from the evaluated CASE.
    control = LTSeq.from_arrow(pa.table({"c": actual})).derive(v=lambda r: op(r.c, CUT))
    assert column(control) == expected
    assert column(v) == expected


@pytest.mark.parametrize("consumer", ["filter", "membership"])
def test_case_timestamp_other_consumers(consumer):
    t = timestamps()
    if consumer == "filter":
        assert column(t.filter(lambda r: branch(r, False) < CUT), "k") == [0, 1]
    else:
        assert column(t.derive(v=lambda r: branch(r, False).is_in([CUT]))) == [False, False, True, None]


@pytest.mark.parametrize("reversed", [False, True])
@pytest.mark.parametrize("staged", [False, True])
def test_case_float_decimal_fill(staged, reversed):
    t = LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0],
        "i": pa.array([None, 1, None], pa.int64()),
        "f": pa.array([float("nan"), None, None], pa.float64()),
    }))

    def c(r):
        return if_else(r.k <= 0, r.f, r.i) if reversed else if_else(r.k > 0, r.i, r.f)

    assert t.derive(c=c).to_arrow().column("c").type == pa.float64()
    if staged:
        v = t.derive(c=c).derive(v=lambda r: r.c.fill_null(Decimal("2.5")))
    else:
        v = t.derive(v=lambda r: c(r).fill_null(Decimal("2.5")))
    out = v.to_arrow().column("v")
    assert out.type == pa.float64()
    values = out.to_pylist()
    assert math.isnan(values[0]) and values[1:] == [1.0, 2.5]


@pytest.mark.parametrize("reversed", [False, True])
@pytest.mark.parametrize("float_value", [1000000.0, 2.5])
def test_coalesce_float_literal_decimal_widening(float_value, reversed):
    t = LTSeq.from_arrow(pa.table({"x": pa.array([None, Decimal("1.23")], pa.decimal128(5, 2))}))
    assert column(t.derive(v=lambda r: coalesce(r.x, float_value))) == [
        Decimal(str(float_value)), Decimal("1.23"),
    ]
    assert column(t.derive(v=lambda r: coalesce(r.x, FINE))) == [FINE, Decimal("1.23")]
    literals = [FINE, float_value] if reversed else [float_value, FINE]
    out = t.derive(v=lambda r: coalesce(r.x, *literals))
    assert column(out) == [FINE if reversed else Decimal(str(float_value)), Decimal("1.23")]
