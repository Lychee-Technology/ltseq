"""Core expression types: Literal, BinOp, UnaryOp."""

import numbers
import reprlib
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from .base import Expr

_SUPPORTED_LITERAL_TYPES = (
    "bool, int, float, str, None, decimal.Decimal, datetime.date, datetime.datetime, "
    "pandas.Timestamp, numpy integer/floating/bool_/datetime64 scalars"
)
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_DECIMAL128_MAX_PRECISION = 38
_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
# Arrow's timestamp units. A numpy datetime64 coarser than seconds is sent in
# seconds, one finer than nanoseconds in nanoseconds when that is exact.
_TIMESTAMP_UNITS = ("s", "ms", "us", "ns")
_NUMPY_COARSE_UNITS = ("Y", "M", "W", "D", "h", "m")


def _type_name(value: Any) -> str:
    value_type = type(value)
    if value_type.__module__ == "builtins":
        return value_type.__qualname__
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _missing_value_error(value: Any) -> ValueError:
    return ValueError(f"{value!r} is not a supported literal; use None for a null value")


def _encode_decimal(value: Decimal) -> dict[str, Any]:
    """A Decimal as its unscaled integer with the precision and scale of its digits."""
    if not value.is_finite():
        raise ValueError(f"Decimal literal {value!r} is not finite")
    sign, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)  # finite Decimals have an int exponent
    # Size the value from its digit tuple before building the unscaled int, so
    # an oversized exponent is refused without allocating a 10**exponent.
    if value.is_zero():
        # A zero has one digit; a positive exponent adds none.
        digit_count, exponent = 1, min(exponent, 0)
    else:
        digit_count = len(digits) + max(exponent, 0)
    scale = max(-exponent, 0)
    precision = max(digit_count, scale)
    if precision > _DECIMAL128_MAX_PRECISION:
        raise ValueError(
            f"Decimal literal {value!r} needs precision {precision}; "
            f"at most {_DECIMAL128_MAX_PRECISION} digits are supported"
        )
    unscaled = int("".join(map(str, digits))) * 10 ** max(exponent, 0)
    return {
        "dtype": "Decimal128",
        "value": -unscaled if sign else unscaled,
        "precision": precision,
        "scale": scale,
    }


def _timezone_name(value: datetime) -> str:
    """The zone of an aware datetime as Arrow names it: an IANA key or ``±HH:MM``."""
    tzinfo = value.tzinfo
    for attribute in ("key", "zone"):  # zoneinfo / pandas, pytz
        name = getattr(tzinfo, attribute, None)
        if isinstance(name, str) and name:
            return name
    if tzinfo is timezone.utc:
        return "UTC"
    # A zone without a name (a fixed offset, dateutil): only its offset at
    # this instant can travel.
    offset = value.utcoffset()
    assert offset is not None
    seconds = int(offset.total_seconds())
    if seconds % 60:
        raise ValueError(
            f"time zone offset {offset} of {value!r} is not a whole number of minutes"
        )
    sign = "-" if seconds < 0 else "+"
    minutes = abs(seconds) // 60
    return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"


def _encode_datetime(value: datetime) -> dict[str, Any]:
    """A datetime as ticks since the epoch in its own unit.

    A ``datetime`` is exact in microseconds; a ``pandas.Timestamp`` keeps its
    unit (nanoseconds by default), so no digit is dropped. An aware value is
    sent as its UTC instant with the name of its zone.
    """
    if value != value:  # pandas.NaT subclasses datetime but is a missing value
        raise _missing_value_error(value)
    tz = None if value.utcoffset() is None else _timezone_name(value)
    if type(value).__module__.startswith("pandas"):
        unit = getattr(value, "unit", "ns")
        # asm8 is the UTC instant as a numpy datetime64 in the Timestamp's
        # own unit; reading its ticks avoids a Timedelta, which overflows
        # beyond the nanosecond range.
        ticks = int(value.asm8.view("int64"))
        return {"dtype": "Timestamp", "value": ticks, "unit": unit, "tz": tz}
    delta = value - _EPOCH_UTC if tz is not None else value - _EPOCH_NAIVE
    ticks = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return {"dtype": "Timestamp", "value": ticks, "unit": "us", "tz": tz}


def _encode_datetime64(value: Any) -> dict[str, Any]:
    """A numpy datetime64 as a naive timestamp in its own unit where Arrow has one."""
    import numpy as np  # only reached for a numpy scalar, so numpy is importable

    if np.isnat(value):
        raise _missing_value_error(value)
    unit, _ = np.datetime_data(value.dtype)
    if unit in _TIMESTAMP_UNITS:
        target = unit
    elif unit in _NUMPY_COARSE_UNITS:
        target = "s"
    else:  # ps, fs, as
        target = "ns"
    converted = value.astype(f"datetime64[{target}]")
    if converted.astype(value.dtype) != value:
        raise ValueError(f"{value!r} cannot be represented in nanoseconds")
    return {"dtype": "Timestamp", "value": int(converted.view("int64")), "unit": target, "tz": None}


def _encode_literal(value: Any) -> dict[str, Any]:
    """Validate a Python constant and encode it as a typed wire payload.

    Every payload field is a plain bool/int/float/str/None of the exact type
    its dtype names, so the Rust side never parses a string.

    Raises:
        TypeError: the value's type is not a supported literal type.
        ValueError: the value does not fit its type's encoding, or is a
            missing-value marker (``pandas.NaT``, ``pandas.NA``).
    """
    if value is None:
        return {"dtype": "Null", "value": None}
    if isinstance(value, bool):
        return {"dtype": "Boolean", "value": value}
    module = type(value).__module__
    if module == "numpy":
        # Checked first: numpy.timedelta64 registers as numbers.Integral, and
        # numpy.bool_ is not a bool. Matched by name so plain values never
        # import numpy.
        name = type(value).__name__
        if name in ("bool_", "bool"):
            return {"dtype": "Boolean", "value": bool(value)}
        if name == "datetime64":
            return _encode_datetime64(value)
        if name == "timedelta64":
            raise _unsupported(value)
    if isinstance(value, numbers.Integral):  # int, IntEnum, numpy integers
        as_int = int(value)
        if not _INT64_MIN <= as_int <= _INT64_MAX:
            raise ValueError(
                f"Integer literal {as_int} is outside the Int64 range "
                f"[{_INT64_MIN}, {_INT64_MAX}]"
            )
        return {"dtype": "Int64", "value": as_int}
    if isinstance(value, numbers.Real) and not isinstance(value, numbers.Rational):
        return {"dtype": "Float64", "value": float(value)}  # float, numpy floats
    if isinstance(value, str):
        return {"dtype": "String", "value": str(value)}
    if isinstance(value, Decimal):
        return _encode_decimal(value)
    if isinstance(value, datetime):  # before date: datetime subclasses date
        return _encode_datetime(value)
    if isinstance(value, date):
        return {"dtype": "Date32", "value": (value - _EPOCH_DATE).days}
    if module.startswith("pandas") and type(value).__name__ == "NAType":
        raise _missing_value_error(value)
    raise _unsupported(value)


def _unsupported(value: Any) -> TypeError:
    return TypeError(
        f"Unsupported literal type {_type_name(value)} ({reprlib.repr(value)}); "
        f"supported literal types: {_SUPPORTED_LITERAL_TYPES}; "
        "use .is_in([...]) for membership"
    )


class LiteralExpr(Expr):
    """
    Represents a constant value: bool, int, float, str, None, Decimal, date,
    datetime (including pandas.Timestamp), or a numpy scalar of those kinds.

    The value is validated and encoded when the literal is created, so an
    unsupported constant in a lambda raises at the operator that used it.

    Attributes:
        value: The Python value

    Raises:
        TypeError: The value's type is not a supported literal type.
        ValueError: The value does not fit its type's encoding (an int outside
            Int64, a non-finite or wider-than-38-digit Decimal, a time zone
            offset with seconds), or is ``pandas.NaT``/``pandas.NA``.
    """

    def __init__(self, value: Any):
        """Initialize a LiteralExpr with a constant value."""
        self._payload = _encode_literal(value)
        self.value = value

    def serialize(self) -> dict[str, Any]:
        """Serialize to a typed payload: ``{"type": "Literal", "dtype": ..., ...}``."""
        return {"type": "Literal", **self._payload}


class BinOpExpr(Expr):
    """
    Represents a binary operation: +, -, >, <, ==, &, |, etc.

    Attributes:
        op (str): Operation name ("Add", "Gt", "And", etc.)
        left (Expr): Left operand
        right (Expr): Right operand
    """

    def __init__(self, op: str, left: Any, right: Any):
        """Initialize a BinOpExpr with operation and operands."""
        self.op = op
        self.left = left
        self.right = right

    def serialize(self) -> dict[str, Any]:
        """Serialize to dict with recursive serialization of operands."""
        return {
            "type": "BinOp",
            "op": self.op,
            "left": self.left.serialize(),
            "right": self.right.serialize(),
        }


class UnaryOpExpr(Expr):
    """
    Represents a unary operation: NOT (~), etc.

    Attributes:
        op (str): Operation name ("Not", etc.)
        operand (Expr): The operand
    """

    def __init__(self, op: str, operand: Any):
        """Initialize a UnaryOpExpr with operation and operand."""
        self.op = op
        self.operand = operand

    def serialize(self) -> dict[str, Any]:
        """Serialize to dict with recursive serialization of operand."""
        return {"type": "UnaryOp", "op": self.op, "operand": self.operand.serialize()}
