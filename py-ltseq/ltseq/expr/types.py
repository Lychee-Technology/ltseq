"""Concrete expression types for LTSeq.

This module re-exports all expression types from their respective modules
for backward compatibility. The main types defined here are:
- ColumnExpr: Column reference (e.g., r.age)
- CallExpr: Function/method call (e.g., r.col.shift(1))

Other types are imported from:
- core_types: LiteralExpr, BinOpExpr, UnaryOpExpr
- accessors: StringAccessor, TemporalAccessor
- lookup_expr: LookupExpr
"""

from typing import Any, Dict, Optional

from .base import Expr
from .accessors import StringAccessor, TemporalAccessor
from .core_types import BinOpExpr, LiteralExpr, UnaryOpExpr
from .lookup_expr import LookupExpr


class ColumnExpr(Expr):
    """
    Represents a column reference, e.g., r.age in a lambda.

    When accessed as r.col.shift(1), __getattr__ returns a callable that
    creates a CallExpr when invoked.

    Attributes:
        name (str): The column name
    """

    def __init__(self, name: str):
        """Initialize a ColumnExpr with a column name."""
        self.name = name

    def serialize(self) -> Dict[str, Any]:
        """Serialize to dict: {"type": "Column", "name": self.name}"""
        return {"type": "Column", "name": self.name}

    def __getattr__(self, method_name: str):
        """
        Handle method calls like r.col.shift(1), r.col.rolling(3), etc.

        Returns a callable that, when invoked with arguments, creates a CallExpr.
        """
        if method_name.startswith("_"):
            raise AttributeError(f"No attribute {method_name}")

        def method_call(*args, **kwargs):
            return CallExpr(method_name, args, kwargs, on=self)

        return method_call

    @property
    def s(self):
        """String accessor for string operations (r.col.s.contains, etc.)"""
        return StringAccessor(self)

    @property
    def dt(self):
        """Temporal accessor for datetime operations (r.col.dt.year, etc.)"""
        return TemporalAccessor(self)


class CallExpr(Expr):
    """
    Represents a function/method call: r.col.shift(1), r.col.rolling(3).sum(), etc.

    Supports both method calls (where 'on' is not None) and standalone functions.
    Supports chaining via __getattr__ returning a callable.

    Attributes:
        func (str): Function/method name
        args (tuple): Positional arguments
        kwargs (dict): Keyword arguments
        on (Expr or None): Object the method is called on (None for functions)
    """

    def __init__(
        self,
        func: str,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        on: Optional[Expr] = None,
    ):
        """Initialize a CallExpr with function name, args, kwargs, and target."""
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.on = on

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of args/kwargs/on.

        Args are serialized if they're Exprs; literals are converted to LiteralExpr first.
        """

        def serialize_value(v):
            if isinstance(v, Expr):
                return v.serialize()
            return LiteralExpr(v).serialize()

        return {
            "type": "Call",
            "func": self.func,
            "args": [serialize_value(arg) for arg in self.args],
            "kwargs": {k: serialize_value(v) for k, v in self.kwargs.items()},
            "on": self.on.serialize() if self.on else None,
        }

    def __getattr__(self, method_name: str):
        """
        Allow chaining: r.col.rolling(3).mean().

        This CallExpr becomes the 'on' of the next CallExpr.
        """
        if method_name.startswith("_"):
            raise AttributeError(f"No attribute {method_name}")

        def chained_call(*args, **kwargs):
            return CallExpr(method_name, args, kwargs, on=self)

        return chained_call

    @property
    def s(self):
        """String accessor for chaining string operations on CallExpr results."""
        return StringAccessor(self)

    @property
    def dt(self):
        """Temporal accessor for chaining temporal operations on CallExpr results."""
        return TemporalAccessor(self)

    def over(
        self,
        partition_by: Optional[Expr] = None,
        order_by: Optional[Expr] = None,
        descending: bool = False,
    ) -> "WindowExpr":
        """
        Apply a window specification to this expression.

        Used with ranking functions (row_number, rank, dense_rank, ntile)
        to specify partitioning and ordering.

        Args:
            partition_by: Column(s) to partition by (optional)
            order_by: Column(s) to order by (required for ranking functions)
            descending: If True, order in descending order (default False)

        Returns:
            WindowExpr with the window specification

        Example:
            >>> row_number().over(order_by=r.date)
            >>> rank().over(partition_by=r.group, order_by=r.score)
            >>> dense_rank().over(partition_by=r.dept, order_by=r.salary, descending=True)
        """
        return WindowExpr(
            self, partition_by=partition_by, order_by=order_by, descending=descending
        )


class WindowExpr(Expr):
    """
    Represents a window function with OVER clause.

    Wraps a ranking function (row_number, rank, dense_rank, ntile) with
    partition_by and order_by specifications.

    Attributes:
        expr (CallExpr): The underlying function call (row_number(), rank(), etc.)
        partition_by (Expr or None): Column(s) to partition by
        order_by (Expr or None): Column(s) to order by
        descending (bool): Whether to order descending
    """

    def __init__(
        self,
        expr: CallExpr,
        partition_by: Optional[Expr] = None,
        order_by: Optional[Expr] = None,
        descending: bool = False,
    ):
        """Initialize a WindowExpr with expression and window specification."""
        self.expr = expr
        self.partition_by = partition_by
        self.order_by = order_by
        self.descending = descending

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with window specification.

        Returns dict with type "Window" and nested expr, partition_by, order_by.
        """
        return {
            "type": "Window",
            "expr": self.expr.serialize(),
            "partition_by": self.partition_by.serialize()
            if self.partition_by
            else None,
            "order_by": self.order_by.serialize() if self.order_by else None,
            "descending": self.descending,
        }


# Re-export all types for backward compatibility
__all__ = [
    "ColumnExpr",
    "CallExpr",
    "WindowExpr",
    "LiteralExpr",
    "BinOpExpr",
    "UnaryOpExpr",
    "LookupExpr",
    "StringAccessor",
    "TemporalAccessor",
]
