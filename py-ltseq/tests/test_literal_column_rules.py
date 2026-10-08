"""A literal compared with an expression is typed by that expression (#145).

DataFusion's coercion rules are written for two columns. For a literal they
lose information: a Decimal against a float column casts the column to
decimal (and fails on NaN), a timestamp literal finer than the column's unit
is truncated, a date against a zoned column means UTC midnight. The
transpiler applies the rules below wherever a comparison is built: row,
window and group dialects, ``is_in``, and non-column operands (decisions D5,
D8, D9, P7, P8, P10 on #145). NULL rows stay NULL under every rule. A literal
next to a column in ``fill_null``, ``coalesce`` and ``if_else`` is typed by
that column the same way, and next to several columns by their common type.
"""

import operator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq, coalesce, if_else

NY = "America/New_York"
UTC = timezone.utc
T, F, N = True, False, None


@pytest.fixture(scope="module")
def t():
    return LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "g": pa.array([1, 1, 2, 2, 3], pa.int64()),
                "f": pa.array([1.0, float("nan"), 3.0, float("inf"), None], pa.float64()),
                "f32": pa.array([0.1, 2.5, 3.0, None, 2.0], pa.float32()),
                "p": pa.array(
                    [Decimal("1.23"), Decimal("1.24"), Decimal("1.50"), None, Decimal("2.00")],
                    pa.decimal128(5, 2),
                ),
                "pw": pa.array(
                    [Decimal("1"), Decimal(10) ** 27, None, Decimal("0.1234567891"), Decimal("2")],
                    pa.decimal128(38, 10),
                ),
                "s": pa.array(["1.5", "2", "abc", None, "10"]),
                "d": pa.array([date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2), None, date(1970, 1, 1)]),
                "d64": pa.array(
                    [date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2), None, date(1970, 1, 1)],
                    pa.date64(),
                ),
                "ts_s": pa.array([0, 1, 2, None, 3], pa.timestamp("s")),
                "ts_us": pa.array([0, 1_000_000, 1_000_001, None, 2_000_000], pa.timestamp("us")),
                "ts_ns": pa.array([0, 1, 2, None, 3], pa.timestamp("ns")),
                "ts_ny": pa.array(
                    [
                        datetime(2024, 1, 1, 4, 59, tzinfo=UTC),  # 23:59 Dec 31 in New York
                        datetime(2024, 1, 1, 5, 0, tzinfo=UTC),  # local midnight
                        datetime(2024, 1, 1, 5, 1, tzinfo=UTC),
                        None,
                        datetime(2024, 3, 10, 7, 30, tzinfo=UTC),
                    ],
                    pa.timestamp("us", tz=NY),
                ),
            }
        )
    ).sort("i")


def _v(t, fn):
    return t.derive(v=fn).to_arrow().column("v").to_pylist()


def _floats(values):
    """Floats with NaN made comparable."""
    return ["nan" if v != v else v for v in values]


# ---- Decimal literal against a float operand: compared as that float type ----


OPS = {
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
}
SWAPPED = {"gt": "lt", "ge": "le", "lt": "gt", "le": "ge", "eq": "eq", "ne": "ne"}


def _compare(op, expr, literal):
    return OPS[op](expr, literal)


def _compare_literal_first(op, expr, literal):
    """The same comparison written with the literal on the left."""
    return OPS[SWAPPED[op]](literal, expr)


@pytest.mark.parametrize("op", sorted(OPS))
def test_decimal_against_float_column_is_the_float(t, op):
    """The column used to be cast to decimal, failing on NaN and infinity."""
    as_float = _v(t, lambda r: _compare(op, r.f, 2.0))
    assert _v(t, lambda r: _compare(op, r.f, Decimal("2"))) == as_float
    assert _v(t, lambda r: _compare_literal_first(op, r.f, Decimal("2"))) == as_float


def test_decimal_against_float32_column_is_the_float32(t):
    assert _v(t, lambda r: r.f32 == Decimal("0.1")) == [T, F, F, N, F]


def test_decimal_in_float_arithmetic_is_the_float(t):
    assert _floats(_v(t, lambda r: r.f + Decimal("1.5"))) == _floats(_v(t, lambda r: r.f + 1.5))
    assert _floats(_v(t, lambda r: r.f // Decimal("2"))) == _floats(_v(t, lambda r: r.f // 2.0))


def test_decimal_against_float_in_is_in(t):
    assert _v(t, lambda r: r.f.is_in([Decimal("3"), Decimal("1")])) == [T, F, T, F, N]


# ---- Decimal literal against a Decimal operand whose widened type would overflow ----


def test_high_scale_decimal_against_wide_decimal_column(t):
    """`(38, 10)` against a scale-19 literal used to overflow the column's cast."""
    lit = Decimal("0.1234567890123456789")
    assert _v(t, lambda r: r.pw > lit) == [T, T, N, T, T]
    assert _v(t, lambda r: r.pw <= lit) == [F, F, N, F, F]
    assert _v(t, lambda r: r.pw == lit) == [F, F, N, F, F]
    assert _v(t, lambda r: r.pw != lit) == [T, T, N, T, T]
    assert _v(t, lambda r: lit < r.pw) == [T, T, N, T, T]


def test_decimal_beyond_the_column_range(t):
    lit = Decimal("9" * 38)
    assert _v(t, lambda r: r.pw > lit) == [F, F, N, F, F]
    assert _v(t, lambda r: r.pw < lit) == [T, T, N, T, T]
    assert _v(t, lambda r: r.pw == Decimal("-" + "9" * 38)) == [F, F, N, F, F]


def test_decimal_in_list_against_wide_decimal_column(t):
    values = [Decimal("0.1234567891"), Decimal("0.1234567890123456789")]
    assert _v(t, lambda r: r.pw.is_in(values)) == [F, F, N, T, F]


def test_decimal_that_widens_within_38_digits_is_left_to_datafusion(t):
    assert _v(t, lambda r: r.p > Decimal("1.236")) == [F, T, T, N, T]
    assert _v(t, lambda r: r.p == Decimal("1.5")) == [F, F, T, N, F]


def test_high_scale_decimal_against_integer_column(t):
    assert _v(t, lambda r: r.i > Decimal("1.5000000000000000000000000000001")) == [F, T, T, T, T]
    assert _v(t, lambda r: r.i == Decimal("2.000000000000000000000000000000")) == [F, T, F, F, F]


BIG = Decimal("123456789012345678901234567890123456")
FINE_123 = Decimal("1.230000000000000000000000000000000")  # 1.23 at scale 33


def test_decimal_in_list_mixing_wide_and_fine_items(t):
    """Each item widens with decimal(5, 2) within 38 digits, but DataFusion
    unifies the whole list: 36 integer digits and 33 fractional ones used to
    overflow at collect. `is_in` agrees with the `==` disjunction."""
    expected = [T, F, F, N, F]
    assert _v(t, lambda r: (r.p == BIG) | (r.p == FINE_123)) == expected
    assert _v(t, lambda r: r.p.is_in([BIG, FINE_123])) == expected
    assert _v(t, lambda r: r.p.is_in([10**18, FINE_123])) == expected
    assert _v(t, lambda r: r.i.is_in([BIG, Decimal("2.0000")])) == [F, T, F, F, F]
    assert _v(t, lambda r: r.i.is_in([BIG, Decimal("1.2345")])) == [F, F, F, F, F]


def test_decimal_values_are_fitted_together(t):
    """`coalesce` unifies all its values at once: two Decimals that each share a
    type with decimal(5, 2) need 69 digits together, which used to overflow at
    collect. A literal the column's type holds exactly takes that type (D-b)."""
    kind, _ = _typed(t, lambda r: coalesce(r.p, FINE_123))
    assert kind == "decimal128(36, 33)"
    # 1.23 takes decimal(5, 2), and the 36-digit integer then shares
    # decimal(38, 2) with the column exactly.
    kind, values = _typed(t, lambda r: coalesce(r.p, BIG, FINE_123))
    assert (kind, values[3]) == ("decimal128(38, 2)", BIG)
    # An integer widens as decimal(20, 0); 1.23 then takes the column's type.
    kind, values = _typed(t, lambda r: coalesce(r.p, 10**18, FINE_123))
    assert (kind, values[3]) == ("decimal128(22, 2)", Decimal(10**18))
    # Next to an integer column: an integral Decimal takes the column's own
    # type, a fractional one is an error instead of an overflow on large values.
    kind, values = _typed(t, lambda r: coalesce(r.i, Decimal("2." + "0" * 33)))
    assert (kind, values[0]) == ("int64", 1)
    with pytest.raises(ValueError, match="does not fit column 'i'"):
        t.derive(v=lambda r: coalesce(r.i, FINE_123))


@pytest.fixture(scope="module")
def pairs():
    """Pairs of columns of different types; per row, neither of a pair is
    set, then only the second, then only the first."""
    return LTSeq.from_arrow(
        pa.table(
            {
                "p": pa.array([None, None, Decimal("1.23")], pa.decimal128(5, 2)),
                "pw": pa.array([None, Decimal(10) ** 27, None], pa.decimal128(38, 10)),
                "i": pa.array([None, None, 1], pa.int64()),
                "f": pa.array([None, float("nan"), None], pa.float64()),
                "d": pa.array([None, None, date(2024, 1, 1)]),
                "ts_s": pa.array([None, 5, None], pa.timestamp("s")),
                "ts_ns": pa.array([None, None, 7], pa.timestamp("ns")),
            }
        )
    )


def _both_orders(first, second, literal):
    return [
        lambda r: coalesce(getattr(r, first), getattr(r, second), literal),
        lambda r: coalesce(getattr(r, second), getattr(r, first), literal),
    ]


@pytest.mark.parametrize(
    "first, second, literal, kind, expected",
    [
        # Scale 20 widens with decimal(5, 2) within 38 digits, but with
        # decimal(38, 10) needs 48: this literal is that column's 0.1234567890.
        (
            "p",
            "pw",
            Decimal("0.12345678900000000000"),
            "decimal128(38, 10)",
            [Decimal("0.1234567890"), Decimal(10) ** 27, Decimal("1.23")],
        ),
        # Typed by the integer column alone, the Decimal cast the float column
        # to decimal, failing on NaN.
        ("i", "f", Decimal("1.5"), "double", [1.5, "nan", 1.0]),
        # Typed by the date column alone, a time of day was an error.
        (
            "d",
            "ts_s",
            datetime(2024, 1, 1, 6),
            "timestamp[ns]",
            [datetime(2024, 1, 1, 6), datetime(1970, 1, 1, 0, 0, 5), datetime(2024, 1, 1)],
        ),
    ],
    ids=["decimals", "int-float", "date-timestamp"],
)
def test_values_are_typed_by_every_column(pairs, first, second, literal, kind, expected):
    """`coalesce` unifies all its values at once, so a literal is typed by the
    columns' common type, whichever column comes first."""
    for fn in _both_orders(first, second, literal):
        got, values = _typed(pairs, fn)
        assert (got, _floats(values)) == (kind, expected)


@pytest.mark.parametrize(
    "first, second, literal, match",
    [
        (
            "p",
            "pw",
            Decimal("0.12345678900000000001"),
            r"does not fit the common type of column '(p|pw)' and column '(pw|p)' \(Decimal128\(38, 10\)\)",
        ),
        ("ts_s", "ts_ns", datetime(3000, 1, 1), r"outside the range of the common type .*\(Timestamp\(ns\)\)"),
    ],
    ids=["decimals", "timestamps"],
)
def test_values_no_common_column_type_holds(pairs, first, second, literal, match):
    """A planning error in either order; with the narrower column first, these
    used to fail at collect, on a column cast or in DataFusion's simplifier."""
    for fn in _both_orders(first, second, literal):
        with pytest.raises(ValueError, match=match):
            pairs.derive(v=fn)


NEG_SCALE = [Decimal("10"), Decimal("-10"), None, Decimal("0")]


@pytest.fixture(scope="module")
def neg_scale():
    """A decimal128(38, -1) column: Arrow allows a negative column scale, the literal wire does not."""
    return LTSeq.from_arrow(
        pa.table({"k": range(4), "x": pa.array(NEG_SCALE, pa.decimal128(38, -1))})
    ).sort("k")


@pytest.mark.parametrize(
    "lit", [Decimal("1E-38"), Decimal("-1E-38"), Decimal("0E-38"), Decimal("15"), Decimal("-15")]
)
def test_negative_scale_column_against_a_finer_literal(neg_scale, lit):
    """A scale-38 literal is 39 digits finer than the column, past any power of
    ten an i128 holds: it used to panic while planning (15 and -15 are within
    reach). Compared exactly, in either operand order."""
    for op in (operator.gt, operator.ge, operator.lt, operator.le, operator.eq, operator.ne):
        assert _v(neg_scale, lambda r: op(r.x, lit)) == [None if v is None else op(v, lit) for v in NEG_SCALE]
        assert _v(neg_scale, lambda r: op(lit, r.x)) == [None if v is None else op(lit, v) for v in NEG_SCALE]
    assert _v(neg_scale, lambda r: r.x.is_in([lit, Decimal("-10")])) == [lit == 10, T, N, lit == 0]


def test_negative_scale_column_value_positions(neg_scale):
    with pytest.raises(ValueError, match="does not fit column 'x'"):
        neg_scale.derive(v=lambda r: r.x.fill_null(Decimal("1E-38")))
    with pytest.raises(ValueError, match=r"column 'x' cannot hold the shift\(\) default"):
        neg_scale.derive(v=lambda r: r.x.shift(1, default=Decimal("1E-38")))
    kind, values = _typed(neg_scale, lambda r: r.x.fill_null(Decimal("0E-38")))
    assert (kind, values) == ("decimal128(38, -1)", [10, -10, 0, 0])
    kind, values = _typed(neg_scale, lambda r: r.x.shift(1, default=Decimal("0E-38")))
    assert (kind, values) == ("decimal128(38, -1)", [0, 10, -10, None])


# ---- Decimal columns of every Arrow width ----


DECIMAL_WIDTHS = [
    pa.decimal32(9, 2),
    pa.decimal64(18, 2),
    pa.decimal128(20, 2),
    pa.decimal256(20, 2),
    pa.decimal256(76, 20),
]


@pytest.mark.parametrize("dtype", DECIMAL_WIDTHS, ids=str)
@pytest.mark.parametrize("fill", [1, 1.5, Decimal("1.5"), Decimal("1.236")], ids=repr)
def test_values_keep_a_decimal_column_of_any_width_exact(dtype, fill):
    """A literal is at most a Decimal128, but a column can be a decimal of any
    width. Next to one, a fill value the shared type holds exactly is kept,
    and so are the column's values (review F1 on #225: Decimal256 refused
    1.5, and `fill_null(1)` rounded Decimal32/64 values to int64)."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([Decimal("1.23"), None], dtype)}))
    for fn in (
        lambda r: r.x.fill_null(fill),
        lambda r: coalesce(r.x, fill),
        lambda r: if_else(r.x.is_null(), fill, r.x),
    ):
        kind, values = _typed(t, fn)
        assert not kind.startswith(("int", "uint")), kind
        assert [Decimal(str(v)) for v in values] == [Decimal("1.23"), Decimal(str(fill))]


@pytest.mark.parametrize("dtype", DECIMAL_WIDTHS, ids=str)
def test_values_hold_a_float_by_its_binary_value(dtype):
    """A float literal is the value its bits encode (decision D-j on #225):
    1.236 is 1.2359999999999999875655…, 52 digits no column here has, so it
    is refused rather than rounded to 1.236; a decimal256(76, 52) holds it."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([Decimal("1.23"), None], dtype)}))
    for fn in (
        lambda r: r.x.fill_null(1.236),
        lambda r: coalesce(r.x, 1.236),
        lambda r: if_else(r.x.is_null(), 1.236, r.x),
    ):
        with pytest.raises(ValueError, match="1.236 does not fit column 'x'"):
            t.derive(v=fn)
    wide = LTSeq.from_arrow(pa.table({"x": pa.array([Decimal("1.23"), None], pa.decimal256(76, 52))}))
    kind, values = _typed(wide, lambda r: r.x.fill_null(1.236))
    assert (kind, values) == ("decimal256(76, 52)", [Decimal("1.23"), Decimal(1.236)])


@pytest.mark.parametrize(
    "dtype, tiny",
    [
        (pa.decimal32(9, 2), ValueError),
        (pa.decimal64(18, 2), ValueError),
        (pa.decimal128(20, 2), ValueError),
        (pa.decimal256(20, 2), "decimal256(56, 38)"),
        (pa.decimal256(76, 20), ValueError),
    ],
    ids=["decimal32", "decimal64", "decimal128", "decimal256-narrow", "decimal256-wide"],
)
def test_values_with_a_scale_38_decimal_at_any_width(dtype, tiny):
    """1E-38 shares a type with the column only where the width has room for
    both (a Decimal256(20, 2) does); elsewhere it is refused, also where
    DataFusion has no common type at all (Decimal32/64 and a scale-38
    literal). A scale-38 literal the column holds exactly takes its type."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([Decimal("1.23"), None], dtype)}))
    fill_tiny = lambda r: r.x.fill_null(Decimal("1E-38"))  # noqa: E731
    if tiny is ValueError:
        with pytest.raises(ValueError, match="does not fit column 'x'"):
            t.derive(v=fill_tiny)
    else:
        assert _typed(t, fill_tiny) == (tiny, [Decimal("1.23"), Decimal("1E-38")])
    _, values = _typed(t, lambda r: r.x.fill_null(Decimal("0.1" + "0" * 37)))
    assert values == [Decimal("1.23"), Decimal("0.1")]


@pytest.mark.parametrize(
    "dtype, kind",
    [
        # 19 integer digits: past what an Int64 holds of every value
        (pa.decimal64(18, -1), "decimal64(18, -1)"),
        (pa.decimal32(9, -10), "decimal32(9, -10)"),
        # Arrow scales a Decimal32 up in an i32 when casting it to Int64
        (pa.decimal32(9, -1), "decimal32(9, -1)"),
        # an Int64 holds every value: DataFusion's common type stays
        (pa.decimal64(17, -1), "int64"),
        (pa.decimal32(8, -1), "int64"),
    ],
    ids=str,
)
def test_integer_fill_for_a_negative_scale_decimal_column(dtype, kind):
    """DataFusion's common type of a Decimal32/64 and an Int64 is int64. It
    is kept only when an Int64 holds every value of the column: `fill_null(0)`
    on `decimal64(18, -1)` used to fail at collect on a value past `i64::MAX`
    (review of b6cc39f on #225)."""
    precision, scale = dtype.precision, dtype.scale
    largest = Decimal(10**precision - 1).scaleb(-scale)
    storage = pa.decimal128(precision, scale)
    column = pa.array([largest, None], storage).cast(dtype)
    t = LTSeq.from_arrow(pa.table({"x": column}))
    out = t.derive(v=lambda r: r.x.fill_null(0)).to_arrow().column("v")
    assert str(out.type) == kind
    assert [Decimal(str(v)) if v is not None else v for v in out.cast(pa.decimal128(38, 0)).to_pylist()] == [
        largest,
        0,
    ]


# ---- Decimal / date / datetime literal against a string operand: an error ----


@pytest.mark.parametrize(
    "literal", [Decimal("1.5"), date(2024, 1, 1), datetime(2024, 1, 1, 6), pd.Timestamp("2024-01-01")]
)
def test_non_string_literal_against_string_column_is_an_error(t, literal):
    with pytest.raises(ValueError, match="column 's' is a string"):
        t.derive(v=lambda r: r.s > literal)
    with pytest.raises(ValueError, match="column 's' is a string"):
        t.derive(v=lambda r: literal == r.s)


# ---- timestamp literal finer than the operand's unit (P7, P8; closes #200) ----

L = datetime(1970, 1, 1, 0, 0, 1, 500000)  # 1.5 s, against seconds [0, 1, 2, null, 3]


@pytest.mark.parametrize(
    "op, expected",
    [
        ("lt", [T, T, F, N, F]),
        ("le", [T, T, F, N, F]),
        ("gt", [F, F, T, N, T]),
        ("ge", [F, F, T, N, T]),
        ("eq", [F, F, F, N, F]),
        ("ne", [T, T, T, N, T]),
    ],
)
def test_finer_timestamp_literal_is_compared_exactly(t, op, expected):
    assert _v(t, lambda r: _compare(op, r.ts_s, L)) == expected
    assert _v(t, lambda r: _compare_literal_first(op, r.ts_s, L)) == expected
    # a non-column operand of the same type
    assert _v(t, lambda r: _compare(op, r.ts_s.fill_null(r.ts_s), L)) == expected


def test_finer_timestamp_literal_in_the_window_dialect(t):
    # shift(-1) of [0, 1, 2, null, 3] is [1, 2, null, 3, null]
    assert _v(t, lambda r: r.ts_s.shift(-1) < L) == [T, F, N, F, N]


def test_finer_timestamp_literal_in_is_in(t):
    later = datetime(1970, 1, 1, 0, 0, 2)
    assert _v(t, lambda r: r.ts_s.is_in([L, later])) == [F, F, T, N, F]
    assert _v(t, lambda r: r.ts_s.is_in([L])) == [F, F, F, N, F]


def test_finer_timestamp_literal_in_the_group_dialect(t):
    # groups by g: {0, 1} max 1, {2, null} max 2, {3} max 3
    kept = t.group_ordered(lambda r: r.g).filter(lambda g: g.max("ts_s") < L).flatten()
    assert sorted(kept.to_arrow().column("i").to_pylist()) == [1, 2]


def test_nanosecond_literal_against_microsecond_column(t):
    cutoff = pd.Timestamp("1970-01-01 00:00:01.000000500")
    assert _v(t, lambda r: r.ts_us < cutoff) == [T, T, F, N, F]
    assert _v(t, lambda r: r.ts_us >= cutoff) == [F, F, T, N, T]
    assert _v(t, lambda r: r.ts_us == cutoff) == [F, F, F, N, F]


def test_timestamp_literal_beyond_the_nanosecond_range(t):
    assert _v(t, lambda r: r.ts_ns < datetime(2300, 1, 1)) == [T, T, T, N, T]
    assert _v(t, lambda r: r.ts_ns > datetime(1600, 1, 1)) == [T, T, T, N, T]


# ---- time zones (D5, P10) ----


def test_date_against_zoned_column_is_local_midnight(t):
    assert _v(t, lambda r: r.ts_ny > date(2024, 1, 1)) == [F, F, T, N, T]
    assert _v(t, lambda r: r.ts_ny >= date(2024, 1, 1)) == [F, T, T, N, T]
    assert _v(t, lambda r: r.ts_ny == date(2024, 1, 1)) == [F, T, F, N, F]


def test_naive_datetime_against_zoned_column_is_wall_clock_time(t):
    assert _v(t, lambda r: r.ts_ny >= datetime(2024, 1, 1)) == [F, T, T, N, T]


def test_naive_datetime_in_a_dst_gap_is_an_error(t):
    with pytest.raises(ValueError, match="2024-03-10 02:30:00 does not exist in America/New_York"):
        t.derive(v=lambda r: r.ts_ny > datetime(2024, 3, 10, 2, 30))


def test_naive_datetime_in_a_dst_fold_is_an_error(t):
    with pytest.raises(ValueError, match="2024-11-03 01:30:00 is ambiguous in America/New_York"):
        t.derive(v=lambda r: r.ts_ny > datetime(2024, 11, 3, 1, 30))


def test_aware_datetime_against_naive_column_is_an_error(t):
    with pytest.raises(ValueError, match="column 'ts_us' is timezone-naive"):
        t.derive(v=lambda r: r.ts_us > datetime(1970, 1, 1, tzinfo=UTC))


def test_aware_datetime_against_zoned_column_compares_instants(t):
    tokyo = datetime(2024, 1, 1, 14, 0, tzinfo=ZoneInfo("Asia/Tokyo"))  # 05:00 UTC
    assert _v(t, lambda r: r.ts_ny > tokyo) == [F, F, T, N, T]


def _ticks(instant):
    delta = instant - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 10**9 + delta.microseconds * 1000


@pytest.mark.parametrize("zone, day", [("Asia/Tokyo", date(2262, 4, 12)), (NY, date(1677, 9, 21))])
def test_local_midnight_inside_nanoseconds_when_naive_midnight_is_not(zone, day):
    """Naive midnight of these days is outside the nanosecond range; local
    midnight (15:00 UTC the day before in Tokyo, 04:56 UTC in 1677 New York)
    is inside. The date used to be taken as beyond every value."""
    midnight = datetime.combine(day, datetime.min.time(), ZoneInfo(zone))
    ticks = _ticks(midnight)
    x = pa.array([ticks - 1, ticks, ticks + 1, None], pa.int64()).cast(pa.timestamp("ns", zone))
    z = LTSeq.from_arrow(pa.table({"k": range(4), "x": x})).sort("k")
    assert _v(z, lambda r: r.x == midnight) == [F, T, F, N]
    assert _v(z, lambda r: r.x == day) == [F, T, F, N]
    assert _v(z, lambda r: r.x > day) == [F, F, T, N]
    assert _v(z, lambda r: r.x <= day) == [T, T, F, N]
    assert _v(z, lambda r: r.x.is_in([day])) == [F, T, F, N]
    # pyarrow cannot turn these values into Python objects; compare ticks.
    filled = z.derive(v=lambda r: r.x.fill_null(day)).to_arrow().column("v")
    assert filled.cast(pa.int64()).to_pylist()[3] == ticks


def test_naive_literal_localized_past_its_own_unit():
    """Wall-clock time west of UTC near the end of the nanosecond range is an
    instant past it. Against a microsecond column the instant is placed at
    microseconds; it used to raise "outside the supported range"."""
    last_us = pd.Timestamp.max.value // 1000
    z = LTSeq.from_arrow(
        pa.table({"k": range(2), "x": pa.array([last_us, None], pa.timestamp("us", NY))})
    ).sort("k")
    assert _v(z, lambda r: r.x < pd.Timestamp.max) == [T, N]
    assert _v(z, lambda r: r.x == pd.Timestamp.max) == [F, N]
    # Exact at microseconds: usable in arithmetic and as a value too.
    late = pd.Timestamp("2262-04-11 23:00")  # 04:00 UTC on the 12th at -05:00
    at_five = datetime(2262, 4, 12, 5, tzinfo=UTC)
    w = LTSeq.from_arrow(
        pa.table({"k": range(2), "x": pa.array([at_five, None], pa.timestamp("us", "-05:00"))})
    ).sort("k")
    assert _v(w, lambda r: r.x - late)[0] == timedelta(hours=1)
    assert _v(w, lambda r: r.x.fill_null(late))[1] == datetime(2262, 4, 12, 4, tzinfo=UTC)


# ---- timestamp literal against a date operand: instants, without the ns range ----


def test_datetime_against_date_column_compares_instants(t):
    assert _v(t, lambda r: r.d == datetime(2024, 1, 1, 6)) == [F, F, F, N, F]
    assert _v(t, lambda r: r.d > datetime(2024, 1, 1, 6)) == [F, F, T, N, F]
    assert _v(t, lambda r: r.d <= datetime(2024, 1, 1, 6)) == [T, T, F, N, T]
    assert _v(t, lambda r: r.d >= datetime(2024, 1, 1)) == [F, T, T, N, F]


def test_datetime_against_date_column_outside_the_nanosecond_range(t):
    assert _v(t, lambda r: r.d > datetime(1600, 1, 1)) == [T, T, T, N, T]
    assert _v(t, lambda r: r.d < datetime(2300, 1, 1)) == [T, T, T, N, T]


# ---- dt.diff with literals (D35) ----


def test_dt_diff_with_literals(t):
    assert _v(t, lambda r: r.ts_us.dt.diff(datetime(1970, 1, 1), unit="second")) == [0.0, 1.0, 1.000001, N, 2.0]
    assert _v(t, lambda r: r.d.dt.diff(date(2024, 1, 1))) == [-1, 0, 1, N, -19723]
    midnight_ny = datetime(2024, 1, 1, tzinfo=ZoneInfo(NY))
    hours = _v(t, lambda r: r.ts_ny.dt.diff(midnight_ny, unit="minute"))
    assert hours[:4] == [-1.0, 0.0, 1.0, N]



# ---- literals in value positions: fill_null, coalesce, if_else ----


def _typed(t, fn):
    out = t.derive(v=fn).to_arrow()
    return str(out.schema.field("v").type), out.column("v").to_pylist()


@pytest.mark.parametrize(
    "make",
    [
        lambda lit: lambda r: r.f.fill_null(lit),
        lambda lit: lambda r: coalesce(r.f, lit),
        lambda lit: lambda r: if_else(r.i > 3, lit, r.f),
    ],
    ids=["fill_null", "coalesce", "if_else"],
)
def test_decimal_value_with_float_column_is_the_float(t, make):
    """The float column used to be cast to decimal, failing on NaN and infinity."""
    kind, values = _typed(t, make(Decimal("2.5")))
    assert (kind, _floats(values)) == ("double", _floats(_v(t, make(2.5))))


def test_date_value_with_zoned_column_is_local_midnight(t):
    midnight = datetime(2024, 1, 1, tzinfo=ZoneInfo(NY))
    kind, values = _typed(t, lambda r: r.ts_ny.fill_null(date(2024, 1, 1)))
    assert kind == "timestamp[us, tz=America/New_York]"
    assert values[3] == midnight
    kind, values = _typed(t, lambda r: if_else(r.i > 3, date(2024, 1, 1), r.ts_ny))
    assert kind == "timestamp[us, tz=America/New_York]"
    assert values[3:] == [midnight, midnight]


def test_naive_value_with_zoned_column_is_wall_clock_time(t):
    kind, values = _typed(t, lambda r: r.ts_ny.fill_null(datetime(2024, 1, 1, 6)))
    assert kind == "timestamp[us, tz=America/New_York]"
    assert values[3] == datetime(2024, 1, 1, 6, tzinfo=ZoneInfo(NY))
    with pytest.raises(ValueError, match="does not exist in America/New_York"):
        t.derive(v=lambda r: r.ts_ny.fill_null(datetime(2024, 3, 10, 2, 30)))


def test_aware_value_with_naive_column_is_an_error(t):
    with pytest.raises(ValueError, match="column 'ts_us' is timezone-naive"):
        t.derive(v=lambda r: r.ts_us.fill_null(datetime(2024, 1, 1, tzinfo=UTC)))


@pytest.mark.parametrize("literal", [Decimal("1.5"), date(2024, 1, 1), datetime(2024, 1, 1, 6)])
def test_non_string_value_with_string_column_is_an_error(t, literal):
    with pytest.raises(ValueError, match="column 's' is a string"):
        t.derive(v=lambda r: r.s.fill_null(literal))
    with pytest.raises(ValueError, match="column 's' is a string"):
        t.derive(v=lambda r: if_else(r.i > 3, literal, r.s))


@pytest.mark.parametrize(
    "fn, message",
    [
        (lambda r: if_else(r.i > 3, date(2024, 1, 1), r.i), "column 'i' is numeric"),
        (lambda r: r.f.fill_null(datetime(2024, 1, 1)), "column 'f' is numeric"),
        (lambda r: r.d.fill_null(Decimal("1.5")), "column 'd' is a date or timestamp"),
    ],
)
def test_mixing_numbers_and_dates_is_an_error(t, fn, message):
    """`if_else(c, date, int_column)` used to cast the integers to dates."""
    with pytest.raises(ValueError, match=message):
        t.derive(v=fn)


def test_wide_decimal_value_is_rescaled_only_when_exact(t):
    kind, values = _typed(t, lambda r: r.pw.fill_null(Decimal("0.12345678900000000000")))
    assert kind == "decimal128(38, 10)"
    assert values[2] == Decimal("0.1234567890")
    with pytest.raises(ValueError, match="does not fit column 'pw'"):
        t.derive(v=lambda r: r.pw.fill_null(Decimal("0.1234567890123456789")))


def test_shift_default_is_typed_by_its_column(t):
    with pytest.raises(ValueError, match="column 's' is a string"):
        t.derive(v=lambda r: r.s.shift(1, default=datetime(2024, 1, 1, 6)))
    kind, values = _typed(t, lambda r: r.f.shift(1, default=Decimal("0.5")))
    assert (kind, values[0]) == ("double", 0.5)
    kind, values = _typed(t, lambda r: r.ts_ny.shift(1, default=date(2024, 1, 1)))
    assert (kind, values[0]) == ("timestamp[us, tz=America/New_York]", datetime(2024, 1, 1, tzinfo=ZoneInfo(NY)))


# ---- Date64 operands ----


def test_datetime_against_date64_column(t):
    assert _v(t, lambda r: r.d64 < datetime(2300, 1, 1)) == [T, T, T, N, T]
    assert _v(t, lambda r: r.d64 > datetime(2024, 1, 1, 6)) == [F, F, T, N, F]
    assert _v(t, lambda r: r.d64 == datetime(2024, 1, 1)) == [F, T, F, N, F]


# ---- review fixes: dates and timestamps in value positions, temporal arithmetic ----


@pytest.fixture(scope="module")
def sentinels():
    """Columns with a far-future sentinel row, which no nanosecond timestamp can hold."""
    return LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "d": pa.array([date(2024, 1, 1), None, date(9999, 12, 31)]),
                "d64": pa.array([date(2024, 1, 1), None, date(9999, 12, 31)], pa.date64()),
                "ts_s": pa.array([datetime(2024, 1, 1), None, datetime(9999, 12, 31)], pa.timestamp("s")),
            }
        )
    ).sort("k")


def test_midnight_datetime_value_with_date_column_is_a_date(sentinels):
    kind, values = _typed(sentinels, lambda r: if_else(r.k == 2, datetime(2024, 1, 2), r.d))
    assert (kind, values) == ("date32[day]", [date(2024, 1, 1), date(2024, 1, 2), date(9999, 12, 31)])
    kind, values = _typed(sentinels, lambda r: r.d64.fill_null(datetime(2024, 1, 2)))
    assert (kind, values) == ("date64[ms]", [date(2024, 1, 1), date(2024, 1, 2), date(9999, 12, 31)])


def test_datetime_value_with_time_of_day_for_date_column_is_an_error(sentinels):
    with pytest.raises(ValueError, match="column 'd' is a date; .* has a time of day"):
        sentinels.derive(v=lambda r: r.d.fill_null(datetime(2024, 1, 2, 6)))


def test_date_value_with_naive_timestamp_column_keeps_the_column_unit(sentinels):
    kind, values = _typed(sentinels, lambda r: r.ts_s.fill_null(date(2024, 1, 2)))
    assert kind == "timestamp[s]"
    assert values == [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(9999, 12, 31)]
    kind, _ = _typed(sentinels, lambda r: r.ts_s.fill_null(datetime(2024, 1, 2, 6)))
    assert kind == "timestamp[s]"


def test_dt_diff_reads_a_literal_like_a_comparison(t):
    # row 2 of ts_ny is 05:00 UTC, local midnight in New York
    assert _v(t, lambda r: r.ts_ny.dt.diff(date(2024, 1, 1), unit="hour"))[1] == 0.0
    assert _v(t, lambda r: r.ts_ny.dt.diff(datetime(2024, 1, 1), unit="hour"))[1] == 0.0
    with pytest.raises(ValueError, match="timezone"):
        t.derive(v=lambda r: r.ts_us.dt.diff(datetime(1970, 1, 1, tzinfo=UTC)))


def test_dt_diff_of_a_date_column_and_an_aware_literal_reads_the_date_like_a_comparison(t):
    # A date column is UTC midnight, in dt.diff as in a comparison; New York
    # midnight on 2024-01-01 is 05:00 UTC.
    ny_midnight = datetime(2024, 1, 1, tzinfo=ZoneInfo(NY))
    assert _v(t, lambda r: r.d.dt.diff(ny_midnight, unit="hour"))[1] == -5.0
    assert _v(t, lambda r: r.d < ny_midnight)[1] is True


def test_dt_diff_between_a_date_and_a_far_datetime(t):
    days = _v(t, lambda r: r.d.dt.diff(datetime(2300, 1, 1)))
    assert days[1] == (date(2024, 1, 1) - date(2300, 1, 1)).days


def test_timestamp_arithmetic_reads_a_literal_like_a_comparison(t):
    from datetime import timedelta

    assert _v(t, lambda r: r.ts_ny - datetime(2024, 1, 1))[1] == timedelta(0)
    with pytest.raises(ValueError, match="column 'ts_us' is timezone-naive"):
        t.derive(v=lambda r: r.ts_us - datetime(1970, 1, 1, tzinfo=UTC))


@pytest.mark.parametrize(
    "fn",
    [
        lambda r: r.i == date(2024, 1, 1),
        lambda r: r.i > datetime(2024, 1, 1),
        lambda r: r.i.is_in([date(2024, 1, 1)]),
        lambda r: r.i.shift(1) > date(2024, 1, 1),
    ],
    ids=["eq", "gt-datetime", "is_in", "window"],
)
def test_date_literal_against_numeric_column_is_an_error(t, fn):
    """It used to compare the integers as days since 1970."""
    with pytest.raises(ValueError, match="is numeric"):
        t.derive(v=fn)


def test_date_literal_against_numeric_column_in_the_group_dialect(t):
    with pytest.raises(ValueError, match="is numeric"):
        t.group_ordered(lambda r: r.g).filter(lambda g: g.max("i") > date(2024, 1, 1)).flatten().to_arrow()


def test_shift_default_must_fit_the_column_exactly(t):
    with pytest.raises(ValueError, match="column 'i' cannot hold"):
        t.derive(v=lambda r: r.i.shift(1, default=Decimal("1.5")))
    with pytest.raises(ValueError, match="column 'p' cannot hold"):
        t.derive(v=lambda r: r.p.shift(1, default=Decimal("1.236")))
    assert _typed(t, lambda r: r.i.shift(1, default=Decimal("2"))) [1][0] == 2
    kind, values = _typed(t, lambda r: r.p.shift(1, default=Decimal("1.5")))
    assert (kind, values[0]) == ("decimal128(5, 2)", Decimal("1.50"))


U64_MAX = 2**64 - 1


@pytest.mark.parametrize(
    "dtype, default, first",
    [
        # integrality, against the column's own bounds and signedness
        (pa.int64(), 1.5, ValueError),
        (pa.int64(), 2.0, 2),
        (pa.int64(), Decimal(2**63), ValueError),
        (pa.uint64(), Decimal(2**63), 2**63),
        (pa.uint64(), Decimal(U64_MAX), U64_MAX),
        (pa.uint64(), Decimal(U64_MAX + 1), ValueError),
        (pa.uint64(), 2.0**63, 2**63),
        (pa.uint64(), -1, ValueError),
        (pa.int8(), 300, ValueError),
        (pa.int8(), -128, -128),
        # the column's scale and precision; a float is the value its bits
        # encode (decision D-j on #225), so 1.1 has more digits than 1.25
        (pa.decimal128(5, 2), 1.236, ValueError),
        (pa.decimal128(5, 2), 1.1, ValueError),
        (pa.decimal128(5, 2), 1.25, Decimal("1.25")),
        (pa.decimal128(5, 2), 1000, ValueError),
        (pa.decimal128(10, -1), Decimal("1.5"), ValueError),
        (pa.decimal128(10, -1), Decimal("20"), Decimal("20")),
        # decimal columns of the other widths (review F1 on #225)
        (pa.decimal32(9, 2), 1.5, Decimal("1.50")),
        (pa.decimal32(9, 2), 1.236, ValueError),
        (pa.decimal32(9, 2), 10**7, ValueError),
        (pa.decimal64(18, 2), 1.5, Decimal("1.50")),
        (pa.decimal64(18, 2), 1.236, ValueError),
        (pa.decimal64(18, -2), 300, Decimal(300)),
        (pa.decimal64(18, -2), 1.5, ValueError),
        (pa.decimal256(20, 2), 1.5, Decimal("1.50")),
        (pa.decimal256(20, 2), 1.236, ValueError),
        (pa.decimal256(76, 20), 1.236, ValueError),
        (pa.decimal256(76, 52), 1.236, Decimal(1.236)),
        (pa.decimal256(76, 20), Decimal(10**37), Decimal(10**37)),
        (pa.decimal256(76, 20), 2.0**200, ValueError),
        (pa.decimal256(76, 0), 2.0**200, Decimal(2**200)),
        # a float column holds the nearest float
        (pa.float32(), 0.1, pytest.approx(0.1)),
    ],
)
def test_shift_default_is_exact_for_the_column_type(dtype, default, first):
    """lag/lead cast the default to the column's type; a default that cast would
    truncate, round or overflow is an error, judged by the column's own type."""
    t = LTSeq.from_arrow(pa.table({"k": [0, 1], "x": pa.array([None, None], dtype)})).sort("k")
    fn = lambda r: r.x.shift(1, default=default)  # noqa: E731
    if first is ValueError:
        with pytest.raises(ValueError, match="column 'x' cannot hold the shift\\(\\) default .* exactly"):
            t.derive(v=fn)
        return
    kind, values = _typed(t, fn)
    assert kind == str(dtype)
    assert values[0] == first


# ---- literal kinds ltseq does not judge, dates in value positions (review of b6cc39f) ----


@pytest.fixture(scope="module")
def kinds():
    return LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([0, 1], pa.int64()),
                "p": pa.array([Decimal("1.00"), None], pa.decimal128(5, 2)),
                "x": pa.array([7, None], pa.int64()),
                "s": pa.array(["a", None]),
                "d": pa.array([date(2024, 1, 1), None]),
                "d64": pa.array([date(2024, 1, 1), None], pa.date64()),
                "ts": pa.array([datetime(2024, 1, 1), None], pa.timestamp("us")),
            }
        )
    ).sort("k")


@pytest.mark.parametrize(
    "fn, kind, values",
    [
        (lambda r: r.p.fill_null("1.5"), "decimal128(5, 2)", [Decimal("1.00"), Decimal("1.50")]),
        (lambda r: r.p.fill_null("2"), "decimal128(5, 2)", [Decimal("1.00"), Decimal("2.00")]),
        (lambda r: coalesce(r.p, "1.5"), "decimal128(5, 2)", [Decimal("1.00"), Decimal("1.50")]),
        (lambda r: r.p.shift(1, default="1.5"), "decimal128(5, 2)", [Decimal("1.50"), Decimal("1.00")]),
        # DataFusion rounds a numeric string to the column's scale; a Decimal is exact.
        (lambda r: r.p.fill_null("1.236"), "decimal128(5, 2)", [Decimal("1.00"), Decimal("1.24")]),
        (lambda r: r.p.fill_null(Decimal("1.236")), "decimal128(6, 3)", [Decimal("1.000"), Decimal("1.236")]),
        (lambda r: r.x.fill_null("05"), "int64", [7, 5]),
        (lambda r: r.x.fill_null("+5"), "int64", [7, 5]),
        (lambda r: r.x.shift(1, default="5"), "int64", [5, 7]),
        (lambda r: r.x.shift(1, default=True), "int64", [1, 7]),
        (lambda r: r.s.shift(1, default=0), "string", ["0", "a"]),
        (lambda r: r.s.shift(1, default=1.5), "string", ["1.5", "a"]),
    ],
)
def test_string_and_boolean_literals_in_value_positions_are_datafusions(kinds, fn, kind, values):
    """A string or Boolean value, or a number next to a string column, is read
    by DataFusion's cast, as before #225 (decision D-h): `"1.5"` used to be
    refused for a decimal(5, 2) column because it came back as `"1.50"`."""
    assert _typed(kinds, fn) == (kind, values)


@pytest.mark.parametrize("fn", [lambda r: r.x.fill_null("abc"), lambda r: r.x.shift(1, default="abc")])
def test_a_string_the_column_cannot_read_is_datafusions_error(kinds, fn):
    with pytest.raises(ValueError, match="Cannot cast string 'abc' to value of Int64 type"):
        kinds.derive(v=fn).to_arrow()


@pytest.mark.parametrize("column", ["d", "d64", "ts"])
@pytest.mark.parametrize("literal", [5, 1.5, Decimal("5"), True], ids=repr)
def test_number_or_boolean_value_for_a_temporal_column_is_an_error(kinds, column, literal):
    """`shift(1, default=5)` on a date column was 1970-01-06, and 5 µs on a
    timestamp column; `fill_null(5)` failed planning."""
    message = f"column '{column}' is a date or timestamp"
    with pytest.raises(ValueError, match=message):
        kinds.derive(v=lambda r: getattr(r, column).fill_null(literal))
    with pytest.raises(ValueError, match=message):
        kinds.derive(v=lambda r: getattr(r, column).shift(1, default=literal))


def _date64_ms(t, fn):
    return t.derive(v=fn).to_arrow().column("v").cast(pa.int64()).to_pylist()


def test_date64_value_holds_only_dates(kinds):
    """A Date64 counts milliseconds, but its values are days: a datetime with
    a time of day used to be stored as a Date64 that equals no date."""
    with pytest.raises(ValueError, match="column 'd64' is a date; .* has a time of day"):
        kinds.derive(v=lambda r: r.d64.fill_null(datetime(2024, 1, 2, 6)))
    with pytest.raises(ValueError, match=r"column 'd64' cannot hold the shift\(\) default"):
        kinds.derive(v=lambda r: r.d64.shift(1, default=datetime(2024, 1, 2, 6)))
    jan_2 = 19_724 * 86_400_000
    assert _date64_ms(kinds, lambda r: r.d64.fill_null(datetime(2024, 1, 2)))[1] == jan_2
    assert _date64_ms(kinds, lambda r: r.d64.fill_null(date(2024, 1, 2)))[1] == jan_2
    assert _date64_ms(kinds, lambda r: r.d64.shift(1, default=date(2024, 1, 2)))[0] == jan_2


TOKYO = timezone(timedelta(hours=9))


def test_aware_value_for_a_date_column_is_its_utc_instant(kinds):
    """As in comparisons (P10), a date column is read at UTC midnight, so an
    aware literal is that date only at a UTC midnight. Arrow's cast took the
    literal's local date: midnight in Tokyo, 15:00 UTC the day before, was
    stored as the Tokyo date."""
    utc_midnight = datetime(2024, 1, 2, 9, tzinfo=TOKYO)
    assert _v(kinds, lambda r: r.d.fill_null(utc_midnight)) == [date(2024, 1, 1), date(2024, 1, 2)]
    assert _v(kinds, lambda r: r.d.shift(1, default=utc_midnight)) == [date(2024, 1, 2), date(2024, 1, 1)]
    assert _v(kinds, lambda r: r.d == datetime(2024, 1, 1, 9, tzinfo=TOKYO)) == [T, N]
    assert _v(kinds, lambda r: r.d == datetime(2024, 1, 1, tzinfo=TOKYO)) == [F, N]
    tokyo_midnight = datetime(2024, 1, 2, tzinfo=TOKYO)
    with pytest.raises(ValueError, match="2024-01-02 00:00:00 \\+09:00 is not a midnight in UTC"):
        kinds.derive(v=lambda r: r.d.fill_null(tokyo_midnight))
    with pytest.raises(ValueError, match=r"column 'd' cannot hold the shift\(\) default"):
        kinds.derive(v=lambda r: r.d.shift(1, default=tokyo_midnight))
