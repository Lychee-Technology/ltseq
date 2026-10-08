"""Regression cases from the review rounds of #225.

Each section is one review's counterexamples, kept as the reviewer wrote
them (expected values, controls and all), so the redesigned literal
typing is held to every case the earlier implementation was. Reviews:

- c4ca298: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6000430239
- c2b465c: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6001286795
- e4217f4: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6002117054
- b22ecab: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6004458626
- 414926b: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6026630607
- f07b855: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6042664484
- d1eb7b3: https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6047435199
"""

import math
import operator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ltseq import LTSeq, coalesce, if_else, ltseq_core, when
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


# ---------------------------------------------------------------------------
# Review of 414926b
# ---------------------------------------------------------------------------
# DataFusion 55 computes a common decimal precision in i8. Next to a scale-38
# literal, a decimal256(76, -14) column needs 76 + 14 + 38 = 128 digits. A
# debug build panicked there (PanicException, which `except Exception` does
# not catch) before exact placement and the shared-value fallback could run.


@pytest.mark.parametrize("control", [False, True], ids=["review", "control"])
@pytest.mark.parametrize("consumer, expected", [
    ("fill_null", [Decimal("1E14"), Decimal(0)]),
    ("coalesce", [Decimal("1E14"), Decimal(0)]),
    ("if_else", [Decimal("1E14"), Decimal(0)]),
    ("gt", [True, None]),
    ("is_in", [False, None]),
])
def test_fine_scale_literal_next_to_a_coarse_decimal256(consumer, expected, control):
    t = LTSeq.from_arrow(pa.table({"x": pa.array([Decimal("1E14"), None], pa.decimal256(76, -14))}))
    zero, tiny = (Decimal(0), Decimal(0)) if control else (Decimal("0E-38"), Decimal("1E-38"))
    fn = {
        "fill_null": lambda r: r.x.fill_null(zero),
        "coalesce": lambda r: coalesce(r.x, zero),
        "if_else": lambda r: if_else(r.x.is_null(), zero, r.x),
        "gt": lambda r: r.x > tiny,
        "is_in": lambda r: r.x.is_in([zero]),
    }[consumer]
    out = t.derive(v=fn).to_arrow().column("v")
    assert out.to_pylist() == expected
    if pa.types.is_decimal(out.type):
        assert out.type == pa.decimal256(76, -14)


def _units(dtype, units):
    """`units` (ints or None) as values of `dtype`, one unit being 10^-scale.
    Built from the unscaled integers: pyarrow cannot write these scales from
    Python Decimals, nor read every one back (see `_decimals`)."""
    width = dtype.bit_width // 8
    data = b"".join((u or 0).to_bytes(width, "little", signed=True) for u in units)
    validity = pa.array([u is not None for u in units]).buffers()[1]
    return pa.Array.from_buffers(dtype, len(units), [validity, pa.py_buffer(data)])


def _decimals(out):
    """A decimal column as Python Decimals, read through decimal256."""
    return out.cast(pa.decimal256(76, out.type.scale)).to_pylist()


# Each type and how many digits it needs next to a scale-38 literal; past
# 127 DataFusion's i8 overflows. decimal256(76, -52) overflows on its own
# precision minus scale, before the literal's scale is added.
COARSE_DECIMALS = [
    pa.decimal256(76, -13),  # 127
    pa.decimal256(76, -14),  # 128
    pa.decimal128(38, -51),  # 127
    pa.decimal128(38, -52),  # 128
    pa.decimal64(18, -72),  # 128
    pa.decimal256(76, -52),  # 76 + 52 = 128
]


@pytest.mark.parametrize("dtype", COARSE_DECIMALS, ids=str)
def test_fine_scale_literals_at_negative_scale_precision_boundaries(dtype):
    """Every consumer the review listed, on both sides of the overflow, in
    the row and window dialects: exact zeros of any scale share the column's
    type, comparisons are exact, and a value the column cannot hold is a
    planning ValueError."""
    units = [1, None, 0, -1]
    t = LTSeq.from_arrow(pa.table({"k": range(4), "x": _units(dtype, units)})).sort("k")
    unit = Fraction(10) ** -dtype.scale
    values = [None if u is None else u * unit for u in units]
    for zero in (Decimal("0E-38"), Decimal("-0E-38"), Decimal("0E-20")):
        for fn in (
            lambda r: r.x.fill_null(zero),
            lambda r: coalesce(r.x, zero),
            lambda r: if_else(r.x.is_null(), zero, r.x),
        ):
            out = t.derive(v=fn).to_arrow().column("v")
            assert out.type == dtype
            assert _decimals(out) == [unit, 0, 0, -unit]
    for literal in (Decimal("1E-38"), Decimal("-1E-38"), Decimal("0E-38")):
        for op in [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge]:
            expected = [None if v is None else op(v, Fraction(literal)) for v in values]
            assert column(t.derive(v=lambda r: op(r.x, literal))) == expected
            assert column(t.derive(v=lambda r: op(literal, r.x))) == [
                None if v is None else op(Fraction(literal), v) for v in values
            ]
            assert column(t.derive(v=lambda r: op(r.x.shift(1), literal))) == [None] + expected[:-1]
    assert column(t.derive(v=lambda r: r.x.is_in([Decimal("0E-38"), Decimal("1E-38")]))) == [
        False, None, True, False,
    ]
    tiny = Decimal("1E-38")
    for fn in (
        lambda r: r.x.fill_null(tiny),
        lambda r: coalesce(r.x, tiny),
        lambda r: if_else(r.x.is_null(), tiny, r.x),
    ):
        with pytest.raises(ValueError, match="does not fit column 'x'"):
            t.derive(v=fn)


def test_a_type_datafusion_cannot_compute_is_an_ordinary_error():
    """Where no type exists (1E14 + 1E-38 needs 128 digits) the result is an
    error, not a panic. Its class is the build's: a debug build reports the
    panic DataFusion's coercion raises as a planning ValueError, and a
    release build, whose i8 arithmetic wraps instead, reports DataFusion's
    own planning error (a RuntimeError). A PanicException is neither."""
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("1E14"), None], pa.decimal256(76, -14)),
        "y": pa.array([Decimal("1E-38"), None], pa.decimal128(38, 38)),
    }))
    for fn in (lambda r: r.x + Decimal("1E-38"), lambda r: r.x + r.y, lambda r: coalesce(r.x, r.y)):
        with pytest.raises((ValueError, RuntimeError)):
            t.derive(v=fn).to_arrow()


# ---------------------------------------------------------------------------
# Review of f07b855
# ---------------------------------------------------------------------------
# A float type that DataFusion proposes for an exact context (an int32 next
# to 2**53 + 1 and 1.5) grants no nearest-float reading; only a float
# context does (D-i). Branches that are all literals go to DataFusion's
# coercion, not to the CaseBuilder's equality check that refused Decimal
# branches of different scales (D-a, D-f). A time of day on the last day a
# date type holds lies beyond the type, not between it and a next day.

PAST_53 = 2**53 + 1


def _plain(dtype):
    """The value type under a dictionary or run-end encoding."""
    while pa.types.is_dictionary(dtype) or pa.types.is_run_end_encoded(dtype):
        dtype = dtype.value_type
    return dtype


def int32_receiver(shape):
    """An int32 value [1, NULL] next to a key, as a plain, dictionary or
    run-end-encoded column, or computed by a CASE over the plain one."""
    i = pa.array([1, None], pa.int32())
    encoded = {"plain": i, "dictionary": i.dictionary_encode(), "run_end": pc.run_end_encode(i)}
    table = pa.table({"k": [0, 1], "i": encoded.get(shape, i)})
    value = (lambda r: if_else(r.k == 0, r.i, r.i)) if shape == "case" else (lambda r: r.i)
    return LTSeq.from_arrow(table), value


@pytest.mark.parametrize("shape", ["plain", "dictionary", "run_end", "case"])
@pytest.mark.parametrize("literals", [
    (PAST_53, 1.5),
    (1.5, PAST_53),
    (-PAST_53, 1.5),
    (PAST_53, 1.5, 7),
    (PAST_53, 7, 0.5),
], ids=lambda ls: "_".join(str(l) for l in ls))
def test_heterogeneous_literals_in_an_exact_context_are_not_rounded(shape, literals):
    """The review's reproducer: DataFusion proposes Float64 for an int32
    next to an Int64 and a Float64 literal, and 2**53 + 1 is not a double.
    Without the float, the exact fallback keeps the integer at Int64."""
    t, value = int32_receiver(shape)
    with pytest.raises(ValueError, match=r"does not fit .* exactly"):
        t.derive(v=lambda r: coalesce(value(r), *literals))
    integers = [x for x in literals if isinstance(x, int)]
    out = t.derive(v=lambda r: coalesce(value(r), *integers)).to_arrow().column("v")
    assert _plain(out.type) == pa.int64()
    assert out.to_pylist() == [1, integers[0]]


def test_a_staged_exact_context_reads_as_the_flat_one():
    t, _ = int32_receiver("plain")
    staged = t.derive(c=lambda r: coalesce(r.i, PAST_53))
    assert staged.to_arrow().column("c").type == pa.int64()
    assert column(staged, "c") == [1, PAST_53]
    for table, fn in [
        (t, lambda r: coalesce(coalesce(r.i, PAST_53), 1.5)),
        (t, lambda r: r.i.fill_null(PAST_53).fill_null(1.5)),
        (staged, lambda r: coalesce(r.c, 1.5)),
        (staged, lambda r: r.c.fill_null(1.5)),
    ]:
        with pytest.raises(ValueError, match=r"does not fit .* \(Int64\) exactly"):
            table.derive(v=fn)


def test_a_float_context_reads_the_nearest_float_however_it_was_staged():
    """D-i's exception belongs to the context: next to a double, 2**53 + 1
    is 2**53, whether the double is a column, an arithmetic result, or an
    int32 already widened by an earlier float literal."""
    t = LTSeq.from_arrow(pa.table({
        "f": pa.array([1.0, None], pa.float64()),
        "i": pa.array([1, None], pa.int32()),
    }))
    for table, fn in [
        (t, lambda r: coalesce(r.f, PAST_53, 1.5)),
        (t, lambda r: coalesce(r.i * 1.0, PAST_53, 1.5)),
        (t, lambda r: coalesce(coalesce(r.i, 0.5), PAST_53)),
        (t.derive(c=lambda r: coalesce(r.i, 0.5)), lambda r: coalesce(r.c, PAST_53)),
    ]:
        out = table.derive(v=fn).to_arrow().column("v")
        assert out.type == pa.float64()
        assert out.to_pylist()[0] == 1.0
    assert column(t.derive(v=lambda r: coalesce(r.f, PAST_53, 1.5))) == [1.0, float(2**53)]


def test_heterogeneous_literals_next_to_a_decimal_are_exact_or_refused():
    """DataFusion widens decimal(5, 2) to decimal(35, 15) for an Int64 and
    a Float64 literal; that holds 2**53 + 1 and 1.5 exactly, and the result
    is kept with those values. 0.1 (0.1000000000000000055…) is not exact
    anywhere a decimal is proposed, so it is refused (D-j)."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([None, Decimal("1.23")], pa.decimal128(5, 2))}))
    for literals in [(PAST_53, 1.5), (1.5, PAST_53)]:
        out = t.derive(v=lambda r: coalesce(r.x, *literals)).to_arrow().column("v")
        assert out.type == pa.decimal128(35, 15)
        assert out.to_pylist() == [Decimal(literals[0]), Decimal("1.23")]
    with pytest.raises(ValueError, match="0.1 does not fit column 'x'"):
        t.derive(v=lambda r: coalesce(r.x, PAST_53, 0.1))


def _wire(value):
    """The type a literal is sent as (#145)."""
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        scale = max(-exponent, 0)
        return pa.decimal128(max(len(digits) + max(exponent, 0), scale), scale)
    return pa.int64() if isinstance(value, int) else pa.float64()


@pytest.mark.parametrize("branches, dtype", [
    ((Decimal("1.5"), Decimal("2.25")), pa.decimal128(3, 2)),
    ((Decimal("1.5"), Decimal("1.50")), pa.decimal128(3, 2)),
    ((Decimal("-1.5"), Decimal("2.25")), pa.decimal128(3, 2)),
    ((Decimal("1.5"), Decimal("1234567890.123456789")), pa.decimal128(19, 9)),
    ((1, Decimal("2.25")), pa.decimal128(22, 2)),
    ((1, 2.5), pa.float64()),
    ((1, 2), pa.int64()),
], ids=lambda x: str(x) if isinstance(x, tuple) else "")
def test_pure_literal_case_branches_take_datafusions_type(branches, dtype):
    """The review's reproducer, and the control it named: the same CASE over
    columns of the literals' own types gives the same type and values."""
    a, b = branches
    t = LTSeq.from_arrow(pa.table({
        "k": [0, 1],
        "a": pa.array([a, a], _wire(a)),
        "b": pa.array([b, b], _wire(b)),
    }))
    out = t.derive(v=lambda r: if_else(r.k == 0, a, b)).to_arrow().column("v")
    control = t.derive(v=lambda r: if_else(r.k == 0, r.a, r.b)).to_arrow().column("v")
    assert out.type == dtype
    assert control.type == dtype
    assert out.to_pylist() == control.to_pylist() == [a, b]


def test_a_null_branch_takes_the_other_branch_type():
    t = LTSeq.from_arrow(pa.table({"k": [0, 1]}))
    out = t.derive(v=lambda r: if_else(r.k == 0, None, Decimal("2.25"))).to_arrow().column("v")
    assert out.type == pa.decimal128(3, 2)
    assert out.to_pylist() == [None, Decimal("2.25")]


def test_when_tails_of_pure_literals_unify():
    t = LTSeq.from_arrow(pa.table({"k": [0, 1, 2]}))
    scales = (Decimal("1.5"), Decimal("2.25"), Decimal("3.125"))
    for fn in (
        lambda r: if_else(r.k == 0, scales[0], if_else(r.k == 1, scales[1], scales[2])),
        lambda r: when(r.k == 0, scales[0]).when(r.k == 1, scales[1]).otherwise(scales[2]),
    ):
        out = t.derive(v=fn).to_arrow().column("v")
        assert out.type == pa.decimal128(4, 3)
        assert out.to_pylist() == list(scales)
    # A column at the head: the tail is one typed value next to it.
    out = t.derive(
        v=lambda r: if_else(r.k == 0, r.k, if_else(r.k == 1, scales[1], scales[2]))
    ).to_arrow().column("v")
    assert out.type == pa.decimal128(23, 3)
    assert out.to_pylist() == [Decimal(0), scales[1], scales[2]]


def test_branches_that_are_all_literals_have_no_context():
    """Two literals share no column with a value, so they keep DataFusion's
    reading (D-f): 2**53 + 1 and 1.5 fold to the double 2**53 and 1.5, as
    ``coalesce(2**53 + 1, 1.5)`` always did; main refused the CASE only
    through the CaseBuilder. Nested under an int32, that double is one
    typed value, which the int32 widens to exactly."""
    t, _ = int32_receiver("plain")
    for fn in (
        lambda r: if_else(r.k == 0, PAST_53, 1.5),
        lambda r: coalesce(PAST_53, 1.5),
    ):
        out = t.derive(v=fn).to_arrow().column("v")
        assert out.type == pa.float64()
        assert out.to_pylist()[0] == float(2**53)
    out = t.derive(v=lambda r: if_else(r.k == 0, r.i, if_else(r.k == 1, PAST_53, 1.5)))
    assert column(out) == [1.0, float(2**53)]


@pytest.mark.parametrize("dtype, last_midnight_ms", [
    (pa.date32(), (2**31 - 1) * 86_400_000),
    (pa.date64(), ((2**63 - 1) // 86_400_000) * 86_400_000),
], ids=str)
def test_a_time_of_day_past_the_last_date_is_beyond_it(dtype, last_midnight_ms):
    """The review's reproducer: one millisecond into the last day has no
    next day to lie before."""
    def holds(ms):
        kind, array, side = ltseq_core._holds(dtype, LiteralExpr(np.datetime64(ms, "ms")).serialize())
        return kind, None if array is None else array[0].value, side

    assert holds(last_midnight_ms + 1) == ("Beyond", None, "Greater")
    assert holds(last_midnight_ms) == ("Exactly", pa.scalar(last_midnight_ms // 86_400_000, pa.date32()).value if dtype == pa.date32() else last_midnight_ms, None)


# ---------------------------------------------------------------------------
# Review of d1eb7b3
# ---------------------------------------------------------------------------
# An aware literal finer than the column and in another zone kept its own
# zone, so DataFusion's order-dependent unification proposed either zone:
# fill_null, column-first coalesce and one CASE order were refused as a zone
# change, the swapped CASE was accepted. A shared value now reads the
# literal as its instant in the receiver's zone, at its own unit, before
# anything unifies; D-i and D-m then judge the unit alone.

NY, TOKYO = "America/New_York", "Asia/Tokyo"
UNITS = ["s", "ms", "us", "ns"]
NS_PER = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}
BASE_NS = 1_704_067_200 * 10**9  # 2024-01-01 00:00:00 UTC, the column's first value
LITERAL_NS = 1_704_164_645 * 10**9  # 2024-01-02 03:04:05 UTC


def _finer(unit):
    return UNITS[UNITS.index(unit) + 1]


def _precision_cases():
    """(column unit, literal unit, nanoseconds past a whole second, result
    unit): the literal at the column's unit, at a finer unit the column
    still holds exactly, one unit finer than the column, and nanoseconds."""
    for unit in ["s", "ms", "us"]:
        yield pytest.param(unit, unit, 0, unit, id=f"{unit}-aligned")
        yield pytest.param(unit, "ns", 0, unit, id=f"{unit}-aligned_ns")
        finer = _finer(unit)
        yield pytest.param(unit, finer, NS_PER[finer], finer, id=f"{unit}-finer_{finer}")
        if finer != "ns":
            yield pytest.param(unit, "ns", 1, "ns", id=f"{unit}-finer_ns")


PRECISIONS = list(_precision_cases())


ZONES = [
    pytest.param(NY, NY, id="same_zone"),
    pytest.param(NY, "UTC", id="utc_into_ny"),
    pytest.param("UTC", NY, id="ny_into_utc"),
    pytest.param(NY, TOKYO, id="tokyo_into_ny"),
]

# Every form puts the literal on row 0 and the column on rows 1 and 2.
COLUMN_FIRST = {
    "fill_null": lambda r, v: r.x.fill_null(v),
    "coalesce": lambda r, v: coalesce(r.x, v),
    "if_else": lambda r, v: if_else(r.c, r.x, v),
    "if_else_negated": lambda r, v: if_else(~r.c, v, r.x),
    "when": lambda r, v: when(r.c, r.x).otherwise(v),
    "when_negated": lambda r, v: when(~r.c, v).otherwise(r.x),
}


def zoned_column(unit, zone, first_ns=BASE_NS):
    first = first_ns // NS_PER[unit]
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 2],
        "c": [False, True, True],
        "x": pa.array([None, first, first + 1], pa.timestamp(unit, zone)),
    })).sort("k")


def aware_literal(unit, zone, past_ns):
    return pd.Timestamp(LITERAL_NS + past_ns, unit="ns", tz="UTC").tz_convert(zone).as_unit(unit)


def ticks(t, fn):
    out = t.derive(v=fn).to_arrow().column("v")
    return out.type, out.cast(pa.int64()).to_pylist()


def outcome(t, fn):
    try:
        return ticks(t, fn)
    except (ValueError, RuntimeError) as e:
        return type(e).__name__, str(e)


@pytest.mark.parametrize("form", list(COLUMN_FIRST))
@pytest.mark.parametrize("receiver, literal_zone", ZONES)
@pytest.mark.parametrize("unit, literal_unit, past_ns, result_unit", PRECISIONS)
def test_an_aware_value_takes_the_receiver_zone_at_the_unit_it_needs(
    unit, literal_unit, past_ns, result_unit, receiver, literal_zone, form
):
    """The receiver's zone, never the literal's; the receiver's unit when it
    holds the literal, else the literal's (D-m); every instant exact."""
    t = zoned_column(unit, receiver)
    literal = aware_literal(literal_unit, literal_zone, past_ns)
    first = BASE_NS // NS_PER[result_unit]
    step = NS_PER[unit] // NS_PER[result_unit]
    assert ticks(t, lambda r: COLUMN_FIRST[form](r, literal)) == (
        pa.timestamp(result_unit, receiver),
        [(LITERAL_NS + past_ns) // NS_PER[result_unit], first, first + step],
    )


@pytest.mark.parametrize("receiver, literal_zone", ZONES)
@pytest.mark.parametrize("unit, literal_unit, past_ns, result_unit", PRECISIONS)
def test_a_literal_first_coalesce_is_legal_at_the_same_type(
    unit, literal_unit, past_ns, result_unit, receiver, literal_zone
):
    """A type-legality control: the literal comes first, so it is every
    row's value, not the column-first result."""
    t = zoned_column(unit, receiver)
    literal = aware_literal(literal_unit, literal_zone, past_ns)
    assert ticks(t, lambda r: coalesce(literal, r.x)) == (
        pa.timestamp(result_unit, receiver),
        [(LITERAL_NS + past_ns) // NS_PER[result_unit]] * 3,
    )


@pytest.mark.parametrize("receiver, literal_zone", ZONES)
@pytest.mark.parametrize("unit, literal_unit, past_ns, result_unit", PRECISIONS)
@pytest.mark.parametrize("far", [False, True], ids=["2024", "year_3000"])
def test_every_column_first_form_has_one_outcome(
    unit, literal_unit, past_ns, result_unit, receiver, literal_zone, far
):
    """Legality and result do not depend on the form or on which CASE branch
    holds the literal, also where widening to nanoseconds puts a year-3000
    column value out of range (D-m accepts that risk; it is one error)."""
    first_ns = 32_503_680_000 * 10**9 if far else BASE_NS
    t = zoned_column(unit, receiver, first_ns)
    literal = aware_literal(literal_unit, literal_zone, past_ns)
    outcomes = {form: outcome(t, lambda r: fn(r, literal)) for form, fn in COLUMN_FIRST.items()}
    assert len({repr(o) for o in outcomes.values()}) == 1, outcomes


@pytest.mark.parametrize("receiver, literal_zone", ZONES)
@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_a_shift_default_of_another_zone_never_widens(unit, receiver, literal_zone):
    """D-c, not D-m: an aware default the column holds is its instant in the
    column's zone and unit; one finer than the column is refused."""
    t = zoned_column(unit, receiver)
    whole = aware_literal(_finer(unit), literal_zone, 0)
    assert ticks(t, lambda r: r.x.shift(1, default=whole)) == (
        pa.timestamp(unit, receiver),
        [LITERAL_NS // NS_PER[unit], None, BASE_NS // NS_PER[unit]],
    )
    finer = aware_literal(_finer(unit), literal_zone, NS_PER[_finer(unit)])
    with pytest.raises(ValueError, match=r"column 'x' cannot hold the shift\(\) default .* exactly"):
        t.derive(v=lambda r: r.x.shift(1, default=finer)).to_arrow()


def test_two_timestamp_columns_are_datafusions_to_unify():
    """No literal, no policy: two columns of different zones keep
    DataFusion's own coercion, operand order and all, as on main (68d6114)."""
    t = LTSeq.from_arrow(pa.table({
        "c": [False, True],
        "a": pa.array([None, 1_704_067_200_000], pa.timestamp("ms", NY)),
        "b": pa.array([1_704_067_200_000_001, 1_704_067_200_000_002], pa.timestamp("us", "UTC")),
    }))
    utc, ny = pa.timestamp("us", "UTC"), pa.timestamp("us", NY)
    assert ticks(t, lambda r: coalesce(r.a, r.b)) == (utc, [1_704_067_200_000_001, 1_704_067_200_000_000])
    assert ticks(t, lambda r: coalesce(r.b, r.a)) == (ny, [1_704_067_200_000_001, 1_704_067_200_000_002])
    assert ticks(t, lambda r: if_else(r.c, r.a, r.b)) == (utc, [1_704_067_200_000_001, 1_704_067_200_000_000])
    assert ticks(t, lambda r: if_else(~r.c, r.b, r.a)) == (ny, [1_704_067_200_000_001, 1_704_067_200_000_000])
