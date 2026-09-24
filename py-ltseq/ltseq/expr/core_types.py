"""Core expression types: Literal, BinOp, UnaryOp."""

import numbers
import reprlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from .base import Expr

_SUPPORTED_LITERAL_TYPES = (
    "bool, int, float, str, None, decimal.Decimal, datetime.date, datetime.datetime"
)

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_DECIMAL128_MAX_PRECISION = 38
_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MICROSECOND = timedelta(microseconds=1)


def _encode_decimal(value: Decimal) -> dict[str, Any]:
    """Encode a Decimal as an unscaled integer with its own precision/scale."""
    if not value.is_finite():
        raise ValueError(f"Decimal literal {value!r} is not finite")
    sign, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)  # finite Decimals have an int exponent
    unscaled = int("".join(map(str, digits)))
    if exponent > 0:
        unscaled *= 10**exponent
        scale = 0
    else:
        scale = -exponent
    if sign:
        unscaled = -unscaled
    precision = max(len(str(abs(unscaled))), scale)
    if precision > _DECIMAL128_MAX_PRECISION:
        raise ValueError(
            f"Decimal literal {value!r} needs precision {precision}; "
            f"at most {_DECIMAL128_MAX_PRECISION} digits are supported"
        )
    return {"value": unscaled, "dtype": "Decimal128", "precision": precision, "scale": scale}


def _encode_datetime(value: datetime) -> dict[str, Any]:
    """Encode a datetime as microseconds since the Unix epoch.

    Naive datetimes stay naive. Aware datetimes are normalized to their UTC
    instant, so they compare correctly against any timezone-aware column.
    """
    if value != value:  # pandas.NaT subclasses datetime but is a missing value
        raise ValueError(f"{value!r} is not a supported literal; use None for a null value")
    if value.utcoffset() is None:
        return {
            "value": (value.replace(tzinfo=None) - _EPOCH_NAIVE) // _ONE_MICROSECOND,
            "dtype": "TimestampMicrosecond",
            "tz": None,
        }
    return {
        "value": (value - _EPOCH_UTC) // _ONE_MICROSECOND,
        "dtype": "TimestampMicrosecond",
        "tz": "UTC",
    }


def _encode_literal(value: Any) -> dict[str, Any]:
    """Validate a Python constant and encode it as a typed wire payload.

    Every payload value is a plain int/float/str/bool/None, so the Rust side
    extracts native types and never re-parses a string.
    """
    if value is None:
        return {"value": None, "dtype": "Null"}
    if isinstance(value, bool):
        return {"value": value, "dtype": "Boolean"}
    if isinstance(value, numbers.Integral):  # int and numpy integers
        as_int = int(value)
        if not _INT64_MIN <= as_int <= _INT64_MAX:
            raise ValueError(
                f"Integer literal {as_int} is outside the Int64 range "
                f"[{_INT64_MIN}, {_INT64_MAX}]"
            )
        return {"value": as_int, "dtype": "Int64"}
    if isinstance(value, numbers.Real) and not isinstance(value, numbers.Rational):
        return {"value": float(value), "dtype": "Float64"}  # float and numpy floats
    if isinstance(value, str):
        return {"value": value, "dtype": "String"}
    if isinstance(value, Decimal):
        return _encode_decimal(value)
    if isinstance(value, datetime):  # before date: datetime subclasses date
        return _encode_datetime(value)
    if isinstance(value, date):
        return {"value": (value - _EPOCH_DATE).days, "dtype": "Date32"}
    value_type = type(value)
    type_name = (
        value_type.__qualname__
        if value_type.__module__ == "builtins"
        else f"{value_type.__module__}.{value_type.__qualname__}"
    )
    raise TypeError(
        f"Unsupported literal type {type_name} "
        f"({reprlib.repr(value)}); supported literal types: {_SUPPORTED_LITERAL_TYPES}"
    )


class LiteralExpr(Expr):
    """
    Represents a constant value: bool, int, float, str, None, Decimal, date,
    or datetime.

    The value is validated when the literal is created, so an unsupported
    constant in a lambda raises right where it is used.

    Attributes:
        value: The Python value

    Raises:
        TypeError: The value's type is not a supported literal type.
        ValueError: The value does not fit its type's wire encoding (an int
            outside Int64, a non-finite Decimal, or a Decimal wider than 38
            digits).
    """

    def __init__(self, value: Any):
        """Initialize a LiteralExpr with a constant value."""
        self._payload = _encode_literal(value)
        self.value = value

    def serialize(self) -> dict[str, Any]:
        """Serialize to a typed dict: ``{"type": "Literal", "value", "dtype", ...}``."""
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
