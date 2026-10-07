"""Oracles for the exactness facts in ``src/transpiler/exact.rs``.

The Rust fact layer answers two questions: what a cast between two Arrow
types can lose (``cast_class``), and whether a type holds a literal's exact
value (``holds``). Both are checked here against ``fractions.Fraction`` and
the hardware's own rounding, over witness values at every type's boundaries:
powers of two around each significand width, powers of ten around each
precision, the finest step of each scale, and the ends of each range.

The oracle does not repeat the Rust arithmetic. A value is representable in
a type when it survives a round trip through that type (``Fraction(float(x))
== x``; ``x * 10**scale`` an integer below ``10**precision``), and a cast's
class is derived from which witnesses of the source type fail that test at
the target. Floats are the binary values their bits encode, never their
decimal text.
"""

from __future__ import annotations

import math
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from ltseq import ltseq_core
from ltseq.expr.core_types import LiteralExpr

# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------


def cast_class(from_type: pa.DataType, to_type: pa.DataType) -> str:
    return ltseq_core._cast_class(from_type, to_type)


def holds(to_type: pa.DataType, literal: Any) -> tuple[str, Any, str | None]:
    kind, array, side = ltseq_core._holds(to_type, LiteralExpr(literal).serialize())
    return kind, array, side


# ---------------------------------------------------------------------------
# The type universe
# ---------------------------------------------------------------------------

INTS = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
]
FLOATS = [pa.float16(), pa.float32(), pa.float64()]
DECIMALS = [
    pa.decimal32(9, 0),
    pa.decimal32(9, 2),
    pa.decimal32(9, 9),
    pa.decimal32(3, -1),
    pa.decimal32(8, -1),
    pa.decimal32(9, -1),
    pa.decimal32(9, -10),
    pa.decimal32(3, 0),
    pa.decimal32(4, 0),
    pa.decimal64(18, 0),
    pa.decimal64(18, 2),
    pa.decimal64(17, -1),
    pa.decimal64(18, -1),
    pa.decimal64(18, -2),
    pa.decimal64(18, -5),
    pa.decimal128(1, -3),
    pa.decimal128(2, -3),
    pa.decimal128(5, 2),
    pa.decimal128(10, -1),
    pa.decimal128(38, -5),
    pa.decimal128(15, 0),
    pa.decimal128(16, 0),
    pa.decimal128(19, 0),
    pa.decimal128(20, 0),
    pa.decimal128(22, 2),
    pa.decimal128(29, 24),
    pa.decimal128(30, 15),
    pa.decimal128(30, 23),
    pa.decimal128(36, 33),
    pa.decimal128(37, -1),
    pa.decimal128(38, -38),
    pa.decimal128(38, -1),
    pa.decimal128(38, 0),
    pa.decimal128(38, 10),
    pa.decimal128(38, 20),
    pa.decimal128(38, 33),
    pa.decimal128(38, 38),
    pa.decimal256(20, 2),
    pa.decimal256(76, 0),
    pa.decimal256(76, 20),
    pa.decimal256(76, 40),
    pa.decimal256(76, 55),
    pa.decimal256(76, 70),
]
NUMBERS = INTS + FLOATS + DECIMALS

# Arrow refuses a decimal whose scale exceeds its precision, so no column or
# scalar of such a type exists; the universe stays within what Arrow holds.
assert all(t.scale <= t.precision for t in DECIMALS)

FLOAT_NUMPY = {pa.float16(): np.float16, pa.float32(): np.float32, pa.float64(): np.float64}

INF = float("inf")
NAN = float("nan")


def bounds(t: pa.DataType) -> tuple[Fraction, Fraction]:
    """The least and greatest finite value of a number type."""
    if pa.types.is_integer(t):
        info = np.iinfo(t.to_pandas_dtype())
        return Fraction(int(info.min)), Fraction(int(info.max))
    if pa.types.is_floating(t):
        top = Fraction(float(np.finfo(FLOAT_NUMPY[t]).max))
        return -top, top
    assert pa.types.is_decimal(t)
    top = Fraction(10**t.precision - 1, 1) / Fraction(10) ** t.scale
    return -top, top


def representable(t: pa.DataType, x: Fraction | float) -> bool:
    """Whether ``x`` is a value of ``t``: it survives a round trip through it."""
    if not isinstance(x, Fraction):  # NaN or an infinity
        return pa.types.is_floating(t)
    if pa.types.is_integer(t):
        lo, hi = bounds(t)
        return x.denominator == 1 and lo <= x <= hi
    if pa.types.is_floating(t):
        with np.errstate(over="ignore"):
            narrowed = FLOAT_NUMPY[t](float(x))
        return bool(math.isfinite(narrowed)) and Fraction(float(narrowed)) == x
    scaled = x * Fraction(10) ** t.scale
    return scaled.denominator == 1 and abs(scaled) < 10**t.precision


def step_down(t: pa.DataType, value: Any) -> Any:
    """The value of a float type just below ``value``."""
    kind = FLOAT_NUMPY[t]
    return np.nextafter(kind(value), kind(-INF))


def step_up(t: pa.DataType, value: Any) -> Any:
    kind = FLOAT_NUMPY[t]
    return np.nextafter(kind(value), kind(INF))


def floor_at(t: pa.DataType, x: Fraction) -> Fraction:
    """The greatest value of ``t`` not above ``x``, for ``x`` within its range."""
    if pa.types.is_integer(t):
        return Fraction(math.floor(x))
    if pa.types.is_decimal(t):
        unit = Fraction(10) ** t.scale
        return Fraction(math.floor(x * unit)) / unit
    # Round with the hardware, then walk to the floor; the walk is at most
    # a couple of steps since rounding lands next to the value.
    with np.errstate(over="ignore"):
        candidate = FLOAT_NUMPY[t](float(x))
    while Fraction(float(candidate)) > x:
        candidate = step_down(t, candidate)
    while Fraction(float(step_up(t, candidate))) <= x:
        candidate = step_up(t, candidate)
    return Fraction(float(candidate))


def finest_step(t: pa.DataType) -> Fraction:
    if pa.types.is_integer(t):
        return Fraction(1)
    if pa.types.is_floating(t):
        return Fraction(float(np.finfo(FLOAT_NUMPY[t]).smallest_subnormal))
    return Fraction(1) / Fraction(10) ** t.scale


def witnesses(t: pa.DataType, near: pa.DataType) -> list[Fraction | float]:
    """Values of ``t`` at its own boundaries and around the bounds of ``near``.

    Every power of two around each significand width, every power of ten
    around each precision, the finest step, the ends of the range, and a few
    consecutive values of ``t`` just inside each bound of ``near`` (so an odd
    multiple of the step lies there when the type has one).
    """
    lo, hi = bounds(t)
    step = finest_step(t)
    candidates: list[Fraction] = [lo, hi, lo + step, hi - step, Fraction(0), step, -step]
    for exponent in (7, 8, 11, 15, 16, 24, 31, 32, 53, 63, 64):
        for offset in (-1, 0, 1):
            candidates.append(Fraction(2**exponent + offset))
            candidates.append(-Fraction(2**exponent + offset))
    for exponent in range(1, 40):
        for offset in (-1, 0, 1):
            candidates.append(Fraction(10**exponent + offset))
    for numerator in (1, 3, 5, 7, 9, 11, 13, 17, 99, 125, 1001):
        candidates.append(Fraction(numerator) * step)
        candidates.append(Fraction(numerator) * step * 1000)
        candidates.append(Fraction(1, numerator))
    near_lo, near_hi = bounds(near)
    for bound in (near_lo, near_hi):
        if lo <= bound <= hi:
            edge = floor_at(t, bound)
            for back in range(5):
                candidates.append(edge - back * step)
                candidates.append(-edge + back * step)
    if pa.types.is_floating(t):
        # The float's own values: the oracle never trusts a decimal text.
        for text in ("0.1", "1.5", "1.1", "1.236", "2.5", "1e-7", "1e8", "1e15", "1e16"):
            candidates.append(Fraction(float(text)))
    values: list[Fraction | float] = []
    seen: set[Fraction] = set()
    for value in candidates:
        if value in seen or not (lo <= value <= hi) or not representable(t, value):
            continue
        seen.add(value)
        values.append(value)
    if pa.types.is_floating(t):
        values.extend([NAN, INF, -INF])
    return values


def arrow_computes(from_type: pa.DataType, to_type: pa.DataType, x: Fraction) -> bool:
    """Whether Arrow's cast reaches a value that both types hold.

    Arrow's decimal-to-integer cast multiplies a negative-scale decimal out in
    the decimal's own storage integer before converting it (arrow-cast,
    ``cast_decimal_to_integer``), so the scaled value must fit that storage.
    """
    if pa.types.is_decimal(from_type) and pa.types.is_integer(to_type) and from_type.scale < 0:
        storage_max = 2 ** (from_type.bit_width - 1) - 1
        return abs(x) <= storage_max
    return True


def classify(range_loss: bool, precision_loss: bool) -> str:
    return {
        (False, False): "Exact",
        (True, False): "RangeOnly",
        (False, True): "Precision",
        (True, True): "RangeAndPrecision",
    }[(range_loss, precision_loss)]


def oracle_cast_class(from_type: pa.DataType, to_type: pa.DataType) -> str:
    if from_type == to_type:
        return "Exact"
    lo, hi = bounds(to_type)
    range_loss = precision_loss = False
    for witness in witnesses(from_type, to_type):
        if not isinstance(witness, Fraction):  # NaN or an infinity
            if not pa.types.is_floating(to_type):
                range_loss = True
            continue
        if not (lo <= witness <= hi) or not arrow_computes(from_type, to_type, witness):
            range_loss = True
        elif not representable(to_type, witness):
            precision_loss = True
    return classify(range_loss, precision_loss)


# ---------------------------------------------------------------------------
# Cast classes
# ---------------------------------------------------------------------------


def _pair_id(pair: tuple[pa.DataType, pa.DataType]) -> str:
    return f"{pair[0]}->{pair[1]}"


NUMBER_PAIRS = [(a, b) for a in NUMBERS for b in NUMBERS]


@pytest.mark.parametrize("pair", NUMBER_PAIRS, ids=_pair_id)
def test_cast_class_matches_witness_oracle(pair: tuple[pa.DataType, pa.DataType]) -> None:
    from_type, to_type = pair
    assert cast_class(from_type, to_type) == oracle_cast_class(from_type, to_type)


@pytest.mark.parametrize(
    ("from_type", "to_type", "expected"),
    [
        # Arrow's cast succeeding is not exactness: these casts all succeed
        # on most values, and each loses something on some value.
        (pa.int64(), pa.float64(), "Precision"),
        (pa.int32(), pa.float64(), "Exact"),
        (pa.uint64(), pa.float64(), "Precision"),
        (pa.int16(), pa.float16(), "Precision"),
        (pa.int8(), pa.float16(), "Exact"),
        (pa.float64(), pa.float32(), "RangeAndPrecision"),
        (pa.float64(), pa.int64(), "RangeAndPrecision"),
        (pa.float64(), pa.decimal128(30, 15), "RangeAndPrecision"),
        (pa.float16(), pa.decimal128(29, 24), "RangeOnly"),
        (pa.decimal128(15, 0), pa.float64(), "Exact"),
        (pa.decimal128(16, 0), pa.float64(), "Precision"),
        (pa.decimal128(2, -3), pa.float64(), "Exact"),
        (pa.decimal128(2, -3), pa.float16(), "RangeAndPrecision"),
        (pa.decimal32(8, -1), pa.int64(), "Exact"),
        (pa.decimal32(9, -1), pa.int64(), "RangeOnly"),
        (pa.decimal64(18, 0), pa.int64(), "Exact"),
        (pa.decimal128(19, 0), pa.int64(), "RangeOnly"),
        (pa.int64(), pa.decimal64(18, -1), "Precision"),
        (pa.decimal128(38, 10), pa.decimal128(38, 20), "RangeOnly"),
        (pa.decimal128(38, 20), pa.decimal128(38, 10), "Precision"),
        # Only zero of these fits the target, so nothing in range rounds.
        (pa.decimal32(9, -10), pa.float16(), "RangeOnly"),
        (pa.decimal128(38, -5), pa.float16(), "RangeOnly"),
        (pa.decimal128(38, -5), pa.float32(), "RangeAndPrecision"),
        (pa.decimal64(18, -5), pa.float32(), "Precision"),
        (pa.decimal32(3, 0), pa.float16(), "Exact"),
        (pa.decimal32(4, 0), pa.float16(), "Precision"),
        (pa.dictionary(pa.int8(), pa.int64()), pa.int64(), "Exact"),
        (pa.dictionary(pa.int8(), pa.int64()), pa.float64(), "Precision"),
    ],
)
def test_cast_class_boundary_witnesses(
    from_type: pa.DataType, to_type: pa.DataType, expected: str
) -> None:
    assert cast_class(from_type, to_type) == expected


@pytest.mark.parametrize(
    ("from_type", "to_type", "expected"),
    [
        (pa.int64(), pa.string(), "Kind"),
        (pa.string(), pa.int64(), "Kind"),
        (pa.bool_(), pa.int64(), "Kind"),
        (pa.int64(), pa.date32(), "Kind"),
        (pa.int64(), pa.timestamp("us"), "Kind"),
        (pa.date32(), pa.int64(), "Kind"),
        (pa.float64(), pa.date32(), "Kind"),
        (pa.int64(), pa.null(), "Kind"),
        (pa.null(), pa.int64(), "Exact"),
        (pa.null(), pa.string(), "Exact"),
        (pa.string(), pa.string(), "Exact"),
        (pa.string(), pa.large_string(), "Unjudged"),
        (pa.bool_(), pa.string(), "Unjudged"),
        (pa.list_(pa.int64()), pa.int64(), "Kind"),
        (pa.timestamp("us"), pa.timestamp("us", tz="UTC"), "Kind"),
        (pa.timestamp("us", tz="UTC"), pa.timestamp("us"), "Kind"),
        (pa.date32(), pa.timestamp("s", tz="UTC"), "Kind"),
    ],
)
def test_cast_class_kinds(from_type: pa.DataType, to_type: pa.DataType, expected: str) -> None:
    assert cast_class(from_type, to_type) == expected


# Instants: a type holds the nanosecond ticks that are a whole number of its
# unit, within its range.
INSTANT_UNIT_NS = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}


def instant_unit_ns(t: pa.DataType) -> int:
    if pa.types.is_date32(t):
        return 86_400 * 10**9
    if pa.types.is_date64(t):
        return 10**6
    return INSTANT_UNIT_NS[t.unit]


def instant_reach_ns(t: pa.DataType) -> int:
    top = 2**31 - 1 if pa.types.is_date32(t) else 2**63 - 1
    return top * instant_unit_ns(t)


def oracle_instant_cast_class(from_type: pa.DataType, to_type: pa.DataType) -> str:
    if from_type == to_type:
        return "Exact"
    zoned = lambda t: pa.types.is_timestamp(t) and t.tz is not None  # noqa: E731
    if zoned(from_type) != zoned(to_type):
        return "Kind"
    unit, reach = instant_unit_ns(from_type), instant_reach_ns(from_type)
    to_unit, to_reach = instant_unit_ns(to_type), instant_reach_ns(to_type)
    ticks = [reach, -reach, unit, 3 * unit, 0]
    range_loss = any(abs(tick) > to_reach for tick in ticks)
    precision_loss = any(abs(tick) <= to_reach and tick % to_unit for tick in ticks)
    return classify(range_loss, precision_loss)


INSTANTS = [
    pa.date32(),
    pa.date64(),
    pa.timestamp("s"),
    pa.timestamp("ms"),
    pa.timestamp("us"),
    pa.timestamp("ns"),
    pa.timestamp("s", tz="UTC"),
    pa.timestamp("us", tz="UTC"),
    pa.timestamp("us", tz="+09:00"),
    pa.timestamp("ns", tz="UTC"),
]


@pytest.mark.parametrize("pair", [(a, b) for a in INSTANTS for b in INSTANTS], ids=_pair_id)
def test_instant_cast_class_matches_oracle(pair: tuple[pa.DataType, pa.DataType]) -> None:
    from_type, to_type = pair
    assert cast_class(from_type, to_type) == oracle_instant_cast_class(from_type, to_type)


# ---------------------------------------------------------------------------
# Holdings
# ---------------------------------------------------------------------------

INT_LITERALS = [
    0,
    1,
    -1,
    127,
    128,
    255,
    256,
    2049,
    65504,
    65505,
    2**24 - 1,
    2**24,
    2**24 + 1,
    2**31 - 1,
    2**31,
    2**53 - 1,
    2**53,
    2**53 + 1,
    -(2**53 + 1),
    10**18,
    2**63 - 1,
    -(2**63),
    999_999_999,
    10**9,
    10**9 + 1,
]
FLOAT_LITERALS = [
    0.0,
    -0.0,
    1.0,
    1.5,
    -1.5,
    0.1,
    -0.1,
    1.1,
    1.236,
    2.5,
    1e8,
    1e-7,
    1e-300,
    -1e-300,
    2.0**53,
    2.0**53 + 2,
    9007199254740993.0,
    2.0**63,
    2.0**64,
    1e19,
    1e39,
    2.0**-149,
    2.0**-150,
    3 * 2.0**-150,
    2.0**-24,
    2.0**-25,
    2049.0,
    65504.0,
    65505.0,
    127.5,
    -128.5,
    255.5,
    -0.5,
    999.5,
    9999.5,
    99999.5,
    2.0**200,
    1e300,
    sys.float_info.max,
    5e-324,
    NAN,
    INF,
    -INF,
]
DECIMAL_LITERALS = [
    Decimal("0"),
    Decimal("1.5"),
    Decimal("-1.5"),
    Decimal("0.1"),
    Decimal("0.10"),
    Decimal("-0.1"),
    Decimal("1.236"),
    Decimal("0.5"),
    Decimal("1.1"),
    Decimal("123.456"),
    Decimal("1E-38"),
    Decimal("9" * 38),
    Decimal("-" + "9" * 38),
    Decimal("1" + "0" * 37),
    Decimal("18446744073709551615"),
    Decimal("9223372036854775808"),
    Decimal("9223372036854775807"),
    Decimal("59604644775390625E-24"),
    Decimal("1250"),
    Decimal("1255"),
    Decimal("12.5"),
    Decimal("127.5"),
    Decimal("-128.5"),
    Decimal("999.995"),
    Decimal("-999.995"),
    Decimal("999.990"),
    Decimal("99"),
    Decimal("9.5E+8"),
    Decimal("9.95E+8"),
    Decimal("9.99999999E+18"),
    Decimal("9.9999999995E+18"),
]
LITERALS = INT_LITERALS + FLOAT_LITERALS + DECIMAL_LITERALS


def exact_value(literal: Any) -> Fraction | float:
    """The exact number a literal denotes: a float by its bits."""
    if isinstance(literal, float) and not math.isfinite(literal):
        return literal
    return Fraction(literal)


def oracle_holding(to_type: pa.DataType, literal: Any) -> tuple[str, Fraction | float | None, str | None]:
    x = exact_value(literal)
    if not isinstance(x, Fraction):  # NaN or an infinity
        if pa.types.is_floating(to_type):
            return "Exactly", x, None
        if math.isnan(x):
            return "NotANumber", None, None
        return "Beyond", None, "Greater" if x > 0 else "Less"
    if representable(to_type, x):
        return "Exactly", x, None
    lo, hi = bounds(to_type)
    if x > hi:
        return "Beyond", None, "Greater"
    if x < lo:
        return "Beyond", None, "Less"
    return "Between", floor_at(to_type, x), None


def held_value(array: pa.Array) -> Fraction | float:
    """The exact value of a one-element array.

    A decimal is read from its buffer, since pyarrow cannot format a scalar
    of negative scale; a float by its bits, through ``float``."""
    if pa.types.is_decimal(array.type):
        width = array.type.bit_width // 8
        unscaled = int.from_bytes(array.buffers()[1][:width].to_pybytes(), "little", signed=True)
        return Fraction(unscaled) / Fraction(10) ** array.type.scale
    value = array[0].as_py()
    if isinstance(value, float) and not math.isfinite(value):
        return value
    return Fraction(value)


def _literal_id(literal: Any) -> str:
    return f"{type(literal).__name__}:{literal!r}"


@pytest.mark.parametrize("to_type", NUMBERS, ids=str)
@pytest.mark.parametrize("literal", LITERALS, ids=_literal_id)
def test_holds_matches_exact_oracle(to_type: pa.DataType, literal: Any) -> None:
    kind, array, side = holds(to_type, literal)
    expected_kind, expected_value, expected_side = oracle_holding(to_type, literal)
    assert kind == expected_kind
    assert side == expected_side
    if expected_value is None:
        assert array is None
    else:
        assert array.type == to_type
        value = held_value(array)
        if isinstance(expected_value, float):
            assert isinstance(value, float)
            assert math.isnan(value) if math.isnan(expected_value) else value == expected_value
        else:
            assert value == expected_value


def test_float_literal_is_its_binary_value() -> None:
    """0.1 is held exactly by a 55-place decimal and placed below 0.1 by a
    15-place one; the text "0.1" plays no part."""
    kind, array, _ = holds(pa.decimal256(76, 55), 0.1)
    assert kind == "Exactly"
    assert array[0].as_py() == Decimal("0.1000000000000000055511151231257827021181583404541015625")
    kind, array, _ = holds(pa.decimal128(30, 15), 0.1)
    assert kind == "Between"
    assert array[0].as_py() == Decimal("0.100000000000000")
    # The same number written as a Decimal is the decimal it says.
    kind, array, _ = holds(pa.decimal128(30, 15), Decimal("0.1"))
    assert kind == "Exactly"
    assert array[0].as_py() == Decimal("0.100000000000000")


def test_float_literals_at_integers_around_the_significand() -> None:
    assert holds(pa.int64(), 9007199254740993.0)[1][0].as_py() == 2**53
    kind, array, _ = holds(pa.float64(), 2**53 + 1)
    assert (kind, array[0].as_py()) == ("Between", float(2**53))
    kind, array, _ = holds(pa.float64(), -(2**53 + 1))
    assert (kind, array[0].as_py()) == ("Between", -float(2**53 + 2))
    kind, array, _ = holds(pa.float32(), 2**24 + 1)
    assert (kind, array[0].as_py()) == ("Between", float(2**24))


@pytest.mark.parametrize(
    ("to_type", "literal", "expected"),
    [
        # A timestamp is its instant; a date is its UTC midnight.
        (pa.date32(), datetime(2024, 1, 1, 6, tzinfo=timezone.utc), ("Between", date(2024, 1, 1))),
        (pa.date32(), datetime(2024, 1, 1, tzinfo=timezone.utc), ("Exactly", date(2024, 1, 1))),
        (
            pa.date32(),
            datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=9))),
            ("Between", date(2023, 12, 31)),
        ),
        (pa.date32(), datetime(2024, 1, 1, 6), ("Between", date(2024, 1, 1))),
        (pa.date64(), datetime(2024, 1, 1, 6), ("Between", date(2024, 1, 1))),
        (pa.date64(), date(2024, 1, 2), ("Exactly", date(2024, 1, 2))),
        (pa.timestamp("s"), datetime(2024, 1, 1, 0, 0, 1, 500000), ("Between", datetime(2024, 1, 1, 0, 0, 1))),
        (pa.timestamp("ms"), datetime(2024, 1, 1, 0, 0, 1, 500000), ("Exactly", datetime(2024, 1, 1, 0, 0, 1, 500000))),
        (pa.timestamp("us"), date(2024, 1, 2), ("Exactly", datetime(2024, 1, 2))),
        (
            pa.timestamp("us", tz="+09:00"),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            ("Exactly", datetime(2024, 1, 1, 9, tzinfo=timezone(timedelta(hours=9)))),
        ),
        # Reading a naive time in a zone, or a date as a zoned instant, is
        # policy, not a fact about the value.
        (pa.timestamp("us", tz="UTC"), datetime(2024, 1, 1), ("Unjudged", None)),
        (pa.timestamp("us"), datetime(2024, 1, 1, tzinfo=timezone.utc), ("Unjudged", None)),
        (pa.timestamp("us", tz="UTC"), date(2024, 1, 1), ("Unjudged", None)),
        # Kinds without arithmetic.
        (pa.date32(), 5, ("Unjudged", None)),
        (pa.timestamp("us"), 5.0, ("Unjudged", None)),
        (pa.int64(), date(2024, 1, 1), ("Unjudged", None)),
        (pa.int64(), "5", ("Unjudged", None)),
        (pa.string(), 5, ("Unjudged", None)),
        (pa.int64(), True, ("Unjudged", None)),
        (pa.bool_(), 1, ("Unjudged", None)),
        # Every type holds its own values and the typed null.
        (pa.string(), "x", ("Exactly", "x")),
        (pa.int64(), None, ("Exactly", None)),
        (pa.decimal128(5, 2), None, ("Exactly", None)),
    ],
)
def test_holds_instants_and_other_kinds(
    to_type: pa.DataType, literal: Any, expected: tuple[str, Any]
) -> None:
    kind, array, side = holds(to_type, literal)
    expected_kind, expected_value = expected
    assert kind == expected_kind
    assert side is None
    if kind == "Unjudged":
        assert array is None
    else:
        assert array.type == to_type
        assert array[0].as_py() == expected_value


def test_probe_rejects_a_non_literal() -> None:
    with pytest.raises(ValueError, match="literal"):
        ltseq_core._holds(pa.int64(), {"type": "Column", "name": "x"})
