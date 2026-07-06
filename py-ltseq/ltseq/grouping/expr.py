"""
GroupExpr expression classes for capturing group-level operations.

These classes capture operations like g.count(), g.first().column, g.max('col')
as serializable expression trees, enabling proxy-based expression capture
that works in all contexts (pytest, REPL, exec).
"""

from abc import ABC, abstractmethod
from typing import Any


class GroupExpr(ABC):
    """Base class for group-level expressions."""

    @abstractmethod
    def serialize(self) -> dict[str, Any]:
        """Serialize this expression to a dictionary."""
        pass

    # Arithmetic operations - return BinOpGroupExpr
    def __add__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(self, "+", other)

    def __radd__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(other, "+", self)

    def __sub__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(self, "-", other)

    def __rsub__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(other, "-", self)

    def __mul__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(self, "*", other)

    def __rmul__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(other, "*", self)

    def __truediv__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(self, "/", other)

    def __rtruediv__(self, other: "GroupExpr") -> "BinOpGroupExpr":
        return BinOpGroupExpr(other, "/", self)


class GroupCountExpr(GroupExpr):
    """Expression for g.count() - count of rows in the group."""

    def serialize(self) -> dict[str, Any]:
        return {"type": "GroupCount"}


class GroupAggExpr(GroupExpr):
    """Expression for group aggregations: g.max('col'), g.median('col'), g.percentile('col', 0.9), ...

    ``arg`` carries the numeric parameter of parameterized aggregates
    (currently only percentile's p); plain aggregates leave it None.
    """

    def __init__(self, func: str, column: str, arg: "float | None" = None):
        self._func = func
        self._column = column
        self._arg = arg

    def serialize(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "type": "GroupAgg",
            "func": self._func,
            "column": self._column,
        }
        if self._arg is not None:
            result["arg"] = self._arg
        return result


class GroupRowColumnExpr(GroupExpr):
    """Expression for g.first().column or g.last().column."""

    def __init__(self, row_type: str, column: str):
        """
        Args:
            row_type: "first" or "last"
            column: column name to access
        """
        self._row_type = row_type
        self._column = column

    def serialize(self) -> dict[str, Any]:
        return {
            "type": "GroupRowColumn",
            "row": self._row_type,
            "column": self._column,
        }


class DeriveRowProxy:
    """
    Proxy for g.first() or g.last() that captures column access.

    When you access an attribute (column) on this proxy, it returns
    a GroupRowColumnExpr capturing the operation.
    """

    def __init__(self, row_type: str):
        """
        Args:
            row_type: "first" or "last"
        """
        self._row_type = row_type

    def __getattr__(self, name: str) -> GroupRowColumnExpr:
        # Avoid capturing internal attributes
        if name.startswith("_"):
            raise AttributeError(name)
        return GroupRowColumnExpr(self._row_type, name)


class BinOpGroupExpr(GroupExpr):
    """Binary operation between two group expressions."""

    def __init__(
        self,
        left: "GroupExpr | int | float",
        op: str,
        right: "GroupExpr | int | float",
    ):
        self._left = left
        self._op = op
        self._right = right

    # Symbolic op (kept internally) → row-dialect op name at the boundary,
    # so the Rust side shares one operator table across both dialects.
    _OP_NAMES = {"+": "Add", "-": "Sub", "*": "Mul", "/": "Div"}

    def _serialize_operand(
        self, operand: "GroupExpr | int | float"
    ) -> dict[str, Any]:
        """Serialize an operand, handling both GroupExpr and literals."""
        if isinstance(operand, GroupExpr):
            return operand.serialize()
        # Literal: share the row dialect's dtype inference.
        from ..expr.core_types import LiteralExpr

        return LiteralExpr(operand).serialize()

    def serialize(self) -> dict[str, Any]:
        return {
            "type": "BinOp",
            "left": self._serialize_operand(self._left),
            "op": self._OP_NAMES.get(self._op, self._op),
            "right": self._serialize_operand(self._right),
        }
