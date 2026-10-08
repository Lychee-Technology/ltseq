"""The exactness decisions taken on #225 (D-i to D-m), each at the examples
it was decided on. The generated grid and families cover the type-position
space; this module is the readable statement of each decision.

- D-i: a literal that shares a result column with a value takes the type
  DataFusion proposes only when that type holds every value and every
  literal exactly; a float literal is no exception.
- D-j: a float literal is the binary value its bits encode. NaN and the
  infinities have no place next to an integer, decimal, date or timestamp.
- D-l: a number or Boolean next to a date or timestamp is a type error in
  every position; `dt.add` is the way to offset one.
- D-m: a shared result column may widen a timestamp to the finer unit a
  literal needs, and nothing else widens; a `shift` default never does.
"""

import datetime
import math
import operator
from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq, coalesce, if_else

UTC = datetime.timezone.utc
NAN, INF = math.nan, math.inf
TWO_53 = 2**53


def _typed(t, fn):
    col = t.derive(v=fn).to_arrow().column("v")
    return str(col.type), col.to_pylist()


def _outcome(t, fn):
    try:
        return _typed(t, fn)
    except ValueError as e:
        return ("ValueError", str(e))


@pytest.fixture(scope="module")
def numbers():
    return LTSeq.from_arrow(pa.table({
        "k": pa.array([0, 1, 2], pa.int64()),
        "i64": pa.array([TWO_53, TWO_53 + 1, None], pa.int64()),
        "i32": pa.array([1, 2, None], pa.int32()),
        "u64": pa.array([2**64 - 1, 0, None], pa.uint64()),
        "dec": pa.array([Decimal("0.10"), Decimal("1.50"), None], pa.decimal128(10, 2)),
        "f": pa.array([0.1, 1.5, None], pa.float64()),
    })).sort("k")


@pytest.fixture(scope="module")
def temporal():
    return LTSeq.from_arrow(pa.table({
        "k": pa.array([0, 1, 2], pa.int64()),
        "d": pa.array([datetime.date(1970, 1, 6), None, datetime.date(2024, 1, 1)], pa.date32()),
        "ts": pa.array([datetime.datetime(2024, 1, 1), None, datetime.datetime(3000, 1, 1)], pa.timestamp("s")),
        "tsz": pa.array([datetime.datetime(2024, 1, 1, tzinfo=UTC), None, None], pa.timestamp("us", tz="UTC")),
        "tsny": pa.array([datetime.datetime(2024, 1, 1, tzinfo=UTC), None, None], pa.timestamp("us", tz="America/New_York")),
    })).sort("k")


SHARED = [
    pytest.param(lambda r, lit: r.i64.fill_null(lit), id="fill_null"),
    pytest.param(lambda r, lit: coalesce(r.i64, lit), id="coalesce"),
    pytest.param(lambda r, lit: if_else(r.k == 2, lit, r.i64), id="if_else"),
]


# ---- D-i: a float literal is no exception in a shared result column ----


@pytest.mark.parametrize("share", SHARED)
@pytest.mark.parametrize("lit", [0.0, -0.0, 2.0**53, 1.0], ids=repr)
def test_int64_keeps_its_type_for_a_float_the_column_holds(numbers, share, lit):
    """D-i: DataFusion proposes Float64, which does not hold 2**53 + 1, so
    the literal takes the column's Int64 instead (and -0.0 is its 0)."""
    kind, values = _typed(numbers, lambda r: share(r, lit))
    assert kind == "int64"
    assert values == [TWO_53, TWO_53 + 1, int(lit)]


@pytest.mark.parametrize("share", SHARED)
@pytest.mark.parametrize("lit", [1.5, 0.1, 2.0**63], ids=repr)
def test_int64_refuses_a_float_it_cannot_hold(numbers, share, lit):
    """D-i: neither Float64 (which rounds the column) nor Int64 (which has
    no 1.5, and ends below 2**63) holds every value, so the plan fails
    instead of rounding."""
    with pytest.raises(ValueError, match="does not fit column 'i64' \\(Int64\\) exactly"):
        numbers.derive(v=lambda r: share(r, lit))


def test_int32_widens_to_float64_for_a_fractional_float(numbers):
    """D-i: Float64 holds every Int32, so DataFusion's proposal stands."""
    assert _typed(numbers, lambda r: coalesce(r.i32, 1.5)) == ("double", [1.0, 2.0, 1.5])
    assert _typed(numbers, lambda r: r.i32.fill_null(1.5)) == ("double", [1.0, 2.0, 1.5])


def test_n_ary_coalesce_is_judged_as_a_whole(numbers):
    assert _typed(numbers, lambda r: coalesce(r.i64, 0.0, 2.0)) == ("int64", [TWO_53, TWO_53 + 1, 0])
    with pytest.raises(ValueError, match="1.5 does not fit column 'i64'"):
        numbers.derive(v=lambda r: coalesce(r.i64, 0.0, 1.5))


def test_decimal_column_holds_a_float_by_its_binary_value(numbers):
    """D-i with D-j: 1.5 is exact at DataFusion's decimal(30, 15) and the
    column widens to it exactly; 0.1 and 1.23 are not exact at any decimal
    the column meets a float at, and the column's own scale has no digits
    for them either."""
    kind, values = _typed(numbers, lambda r: r.dec.fill_null(1.5))
    assert kind == "decimal128(30, 15)"
    assert values == [Decimal("0.1"), Decimal("1.5"), Decimal("1.5")]
    for lit in (0.1, 1.23):
        with pytest.raises(ValueError, match=f"{lit} does not fit column 'dec'"):
            numbers.derive(v=lambda r: r.dec.fill_null(lit))


def test_float_column_keeps_float_semantics(numbers):
    assert _typed(numbers, lambda r: r.f.fill_null(1)) == ("double", [0.1, 1.5, 1.0])
    kind, values = _typed(numbers, lambda r: r.f.fill_null(NAN))
    assert kind == "double" and math.isnan(values[2])


# ---- D-j: a float literal is its binary value ----


@pytest.mark.parametrize(
    "fn, expected",
    [
        (lambda r: r.i64 == 2.0**53, [True, False, None]),
        (lambda r: r.i64 == float(TWO_53 + 1), [True, False, None]),  # float(2**53 + 1) is 2**53
        (lambda r: r.i64 > 2.0**53, [False, True, None]),
        (lambda r: r.i64 != 2.0**53, [False, True, None]),
        (lambda r: r.i64 > 1.5, [True, True, None]),
        (lambda r: r.u64 == 2.0**64, [False, False, None]),
        (lambda r: r.u64 > float(2**64 - 1), [False, False, None]),  # float(2**64 - 1) is 2**64
        (lambda r: r.dec == 0.1, [False, False, None]),  # 0.1 is 0.1000000000000000055…, not 0.10
        (lambda r: r.dec > 0.1, [False, True, None]),
        (lambda r: r.dec < 0.1, [True, False, None]),
        (lambda r: r.dec == 1.5, [False, True, None]),
        (lambda r: r.dec == -0.0, [False, False, None]),
        (lambda r: r.dec == Decimal("0.1"), [True, False, None]),
        (lambda r: r.f == 0.1, [True, False, None]),
        (lambda r: r.f < INF, [True, True, None]),
        (lambda r: r.f == NAN, [False, False, None]),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_comparisons_use_the_literal_value_the_bits_encode(numbers, fn, expected):
    assert _typed(numbers, fn) == ("bool", expected)


@pytest.mark.parametrize("op", [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge])
@pytest.mark.parametrize("lit", [0.1, 1.5, 2.0**53, -0.0], ids=repr)
@pytest.mark.parametrize("name", ["i64", "dec", "f"])
def test_comparison_mirror(numbers, name, lit, op):
    """`L op x` is `x (mirror of op) L`, and a one-item `is_in` is `==`."""
    mirror = {operator.lt: operator.gt, operator.gt: operator.lt, operator.le: operator.ge, operator.ge: operator.le}
    right = _typed(numbers, lambda r: op(getattr(r, name), lit))
    left = _typed(numbers, lambda r: mirror.get(op, op)(lit, getattr(r, name)))
    assert left == right
    if op is operator.eq:
        assert _typed(numbers, lambda r: getattr(r, name).is_in([lit])) == right


@pytest.mark.parametrize("lit", [NAN, INF, -INF], ids=["nan", "inf", "-inf"])
@pytest.mark.parametrize("name", ["i64", "u64", "dec"])
def test_nan_and_infinity_are_refused_next_to_an_exact_domain(numbers, name, lit):
    """D-j: no integer or decimal equals, exceeds or falls below NaN or an
    infinity, in a comparison, `is_in` or a shared value alike."""
    for fn in (
        lambda r: getattr(r, name) == lit,
        lambda r: getattr(r, name) != lit,
        lambda r: getattr(r, name) < lit,
        lambda r: lit <= getattr(r, name),
        lambda r: getattr(r, name).is_in([lit]),
        lambda r: getattr(r, name).is_in([1.0, lit]),
        lambda r: getattr(r, name).fill_null(lit),
        lambda r: coalesce(getattr(r, name), lit),
        lambda r: if_else(r.k == 0, lit, getattr(r, name)),
    ):
        with pytest.raises(ValueError, match="has no value for"):
            numbers.derive(v=fn)


def test_arithmetic_keeps_datafusion_float_semantics(numbers):
    """A float in arithmetic with an integer is DataFusion's Float64 sum."""
    assert _typed(numbers, lambda r: r.i64 + 1.5) == ("double", [2.0**53 + 2, 2.0**53 + 2, None])
    kind, values = _typed(numbers, lambda r: r.i64 + NAN)
    assert kind == "double" and math.isnan(values[0])


# ---- D-l: a number is not a date, in any position ----


DATE_POSITIONS = [
    pytest.param(lambda r, c, lit: c == lit, id="eq"),
    pytest.param(lambda r, c, lit: c > lit, id="gt"),
    pytest.param(lambda r, c, lit: lit < c, id="lt_mirror"),
    pytest.param(lambda r, c, lit: c.is_in([lit]), id="is_in"),
    pytest.param(lambda r, c, lit: c.is_in([lit, datetime.date(2024, 1, 1)]), id="is_in_mixed"),
    pytest.param(lambda r, c, lit: c.fill_null(lit), id="fill_null"),
    pytest.param(lambda r, c, lit: coalesce(c, lit), id="coalesce"),
    pytest.param(lambda r, c, lit: if_else(r.k == 0, lit, c), id="if_else"),
    pytest.param(lambda r, c, lit: c.shift(1, default=lit), id="shift_default"),
    pytest.param(lambda r, c, lit: c + lit, id="add"),
    pytest.param(lambda r, c, lit: lit + c, id="add_mirror"),
    pytest.param(lambda r, c, lit: c - lit, id="sub"),
]


@pytest.mark.parametrize("position", DATE_POSITIONS)
@pytest.mark.parametrize("lit", [0, 5, 1.5, True], ids=repr)
@pytest.mark.parametrize("name", ["d", "ts", "tsz"])
def test_a_number_next_to_a_date_or_timestamp_is_refused_everywhere(temporal, name, lit, position):
    """D-l: `d == 0` is not the epoch day and `d.fill_null(0)` is not
    1970-01-01; the error is the same in every position."""
    with pytest.raises(ValueError, match=f"column '{name}' is a date or timestamp .*; use a date or datetime"):
        temporal.derive(v=lambda r: position(r, getattr(r, name), lit))


def test_a_date_next_to_a_number_is_refused_too(numbers):
    """D-l, mirrored: an Int64 column is no count of days to add to a date."""
    for fn in (lambda r: r.i64 + datetime.date(2024, 1, 1), lambda r: r.i64 == datetime.date(2024, 1, 1)):
        with pytest.raises(ValueError, match="column 'i64' is numeric"):
            numbers.derive(v=fn)


def test_dt_add_is_the_documented_offset(temporal):
    assert _typed(temporal, lambda r: r.d.dt.add(days=1)) == (
        "date32[day]",
        [datetime.date(1970, 1, 7), None, datetime.date(2024, 1, 2)],
    )
    assert _typed(temporal, lambda r: r.ts.dt.add(seconds=1))[1][0] == datetime.datetime(2024, 1, 1, 0, 0, 1)


# ---- D-m: a shared value may widen a timestamp to a finer unit, and nothing else ----

HALF_PAST = datetime.datetime(2024, 1, 1, 0, 0, 1, 500000)


@pytest.mark.parametrize(
    "fn",
    [lambda r: r.ts.fill_null(HALF_PAST), lambda r: coalesce(r.ts, HALF_PAST), lambda r: if_else(r.k == 1, HALF_PAST, r.ts)],
    ids=["fill_null", "coalesce", "if_else"],
)
def test_a_timestamp_column_widens_to_the_unit_a_literal_needs(temporal, fn):
    """D-m: timestamp[s] to timestamp[us] loses no precision and the far
    value (year 3000) survives the microsecond range, so the literal's
    half second is kept rather than refused."""
    assert _typed(temporal, fn) == (
        "timestamp[us]",
        [datetime.datetime(2024, 1, 1), HALF_PAST, datetime.datetime(3000, 1, 1)],
    )


def test_a_timestamp_literal_at_the_column_unit_keeps_the_column_unit(temporal):
    whole = datetime.datetime(2024, 1, 1, 0, 0, 1)
    assert _typed(temporal, lambda r: r.ts.fill_null(whole))[0] == "timestamp[s]"
    assert _typed(temporal, lambda r: r.ts.shift(1, default=whole))[0] == "timestamp[s]"


def test_a_shift_default_never_widens(temporal):
    """D-m stops at `shift`: its default must be a value of the column type."""
    with pytest.raises(ValueError, match="column 'ts' cannot hold the shift\\(\\) default .* exactly"):
        temporal.derive(v=lambda r: r.ts.shift(1, default=HALF_PAST))


def test_widening_does_not_cross_kinds_or_zones(temporal):
    """D-m is for one timestamp unit to a finer one only: a date does not
    become a timestamp for a datetime with a time of day, and a naive
    column does not take a zoned literal."""
    with pytest.raises(ValueError, match="column 'd' is a date; the datetime literal .* has a time of day"):
        temporal.derive(v=lambda r: r.d.fill_null(datetime.datetime(2024, 1, 1, 12)))
    assert _typed(temporal, lambda r: r.d.fill_null(datetime.datetime(2024, 1, 1)))[0] == "date32[day]"
    with pytest.raises(ValueError, match="column 'ts' is timezone-naive, but the literal is timezone-aware"):
        temporal.derive(v=lambda r: r.ts.fill_null(datetime.datetime(2024, 1, 1, tzinfo=UTC)))


NOON_UTC = datetime.datetime(2024, 1, 2, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    "share",
    [
        pytest.param(lambda col, lit: col.fill_null(lit), id="fill_null"),
        pytest.param(lambda col, lit: coalesce(col, lit), id="coalesce"),
        pytest.param(lambda col, lit: coalesce(lit, col), id="coalesce_rev"),
        pytest.param(lambda col, lit: if_else(col.is_null(), lit, col), id="if_else"),
    ],
)
@pytest.mark.parametrize("name", ["tsny", "tsz"])
def test_an_aware_value_of_another_zone_keeps_the_column_zone(temporal, name, share):
    """A change of zone is a change of kind, so a shared result column never
    takes the literal's zone: the literal is the same instant in the
    column's zone. (The literal's zone is whichever the column is not.)"""
    column_zone = {"tsny": "America/New_York", "tsz": "UTC"}[name]
    literal = NOON_UTC if name == "tsny" else NOON_UTC.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
    typ, values = _typed(temporal, lambda r: share(getattr(r, name), literal))
    assert typ == f"timestamp[us, tz={column_zone}]"
    # `coalesce(literal, column)` is the literal on every row.
    assert values[1:] == [NOON_UTC, NOON_UTC]
    assert values[0] in (datetime.datetime(2024, 1, 1, tzinfo=UTC), NOON_UTC)
    assert {str(v.tzinfo) for v in values} == {column_zone}


def test_a_comparison_never_widens_the_column(temporal):
    """A timestamp[s] column compared with a half-second literal is decided
    on the literal's place between two seconds; the column is not cast."""
    assert _typed(temporal, lambda r: r.ts == HALF_PAST) == ("bool", [False, None, False])
    assert _typed(temporal, lambda r: r.ts > HALF_PAST) == ("bool", [False, None, True])
    assert _typed(temporal, lambda r: r.ts < HALF_PAST) == ("bool", [True, None, False])
