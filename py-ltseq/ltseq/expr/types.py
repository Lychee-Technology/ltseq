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

from typing import Any

from .base import Expr
from .accessors import StringAccessor, TemporalAccessor
from .core_types import BinOpExpr, LiteralExpr, UnaryOpExpr
from .lookup_expr import LookupExpr

# Ranking window functions — order/partition come exclusively from `.over()`.
_RANKING_FUNCS = frozenset({"row_number", "rank", "dense_rank", "ntile"})

# Sequence windows accept `.over()` as a unified entry (issue #117) in addition
# to the legacy `partition_by=` kwarg. `_is_sequence_window` mirrors the Rust
# `is_window_call` (src/transpiler/mod.rs) so both sides agree on what counts as
# a window. Rolling aggregates surface as e.g. `rolling(n).mean()`, so the
# aggregate call's `.on` must be a `rolling(...)` call.
_SEQ_WINDOW_FUNCS = frozenset({"shift", "diff", "cum_sum", "cum_max", "cum_min"})
_ROLLING_AGG_FUNCS = frozenset({"mean", "sum", "min", "max", "count", "std"})


def _is_sequence_window(call: "CallExpr") -> bool:
    """True if `call` is a sequence window (shift/diff/cum_*, or rolling(n).agg())."""
    if call.func in _SEQ_WINDOW_FUNCS:
        return True
    if (
        call.func in _ROLLING_AGG_FUNCS
        and isinstance(call.on, CallExpr)
        and call.on.func == "rolling"
    ):
        return True
    return False


class ColumnExpr(Expr):
    """
    Represents a column reference, e.g., r.age in a lambda.

    When accessed as r.col.shift(1), __getattr__ returns a callable that
    creates a CallExpr when invoked.

    Attributes:
        name (str): The column name
    """

    name: str

    def __init__(self, name: str):
        """Initialize a ColumnExpr with a column name."""
        self.name = name

    def serialize(self) -> dict[str, Any]:
        """Serialize to dict: {"type": "Column", "name": self.name}"""
        return {"type": "Column", "name": self.name}

    def over(self, *args, **kwargs):
        """A bare column is never a window expression — `.over()` needs a window
        function first (ranking or sequence). Guard here so `r.age.over(...)`
        raises a clear ValueError instead of being turned into a generic
        ``CallExpr("over")`` by ``__getattr__``."""
        raise ValueError(
            f".over() 只用于窗口表达式（排名函数或序列窗口 shift/rolling/diff/cum_*）；"
            f"列 {self.name!r} 不是窗口表达式，不能加 .over()。"
        )

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
    def str(self):
        """Alias for .s, matching the Pandas/Polars .str accessor convention."""
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
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        on: Any | None = None,
    ):
        """Initialize a CallExpr with function name, args, kwargs, and target."""
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.on = on

    def serialize(self) -> dict[str, Any]:
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
    def str(self):
        """Alias for .s, matching the Pandas/Polars .str accessor convention."""
        return StringAccessor(self)

    @property
    def dt(self):
        """Temporal accessor for chaining temporal operations on CallExpr results."""
        return TemporalAccessor(self)

    def over(
        self,
        partition_by: "Expr | None" = None,
        order_by: "Expr | None" = None,
        descending: "bool | None" = None,
        desc: "bool | None" = None,
    ) -> "WindowExpr":
        """
        Apply a window specification to this expression.

        Works with both ranking functions (row_number, rank, dense_rank, ntile)
        and sequence windows (shift, diff, cum_*, rolling(n).agg()). Window
        expressions default to table order; ``.over()`` overrides partition/order.

        Args:
            partition_by: Column(s) to partition by (optional)
            order_by: Column(s) to order by. Required for ranking functions;
                optional for sequence windows (they fall back to the table sort).
            descending: If True, order in descending order (default False)
            desc: Alias for descending. When both are given, ``descending``
                takes precedence — matching sort()'s resolution so the same
                call behaves identically in both APIs.

        Returns:
            WindowExpr with the window specification

        Example:
            >>> row_number().over(order_by=r.date)
            >>> rank().over(partition_by=r.group, order_by=r.score)
            >>> r.close.shift(1).over(partition_by=r.symbol)
            >>> r.price.cum_max().over(partition_by=r.symbol, order_by=r.date)

        Raises:
            ValueError: if called on a non-window expression, or if
                ``partition_by`` is supplied here while the sequence window also
                carries a ``partition_by=`` kwarg (pick one).
        """
        if self.func not in _RANKING_FUNCS and not _is_sequence_window(self):
            raise ValueError(
                f".over() 只用于窗口表达式（排名函数或序列窗口 shift/rolling/diff/cum_*）；"
                f"{self.func}() 不是窗口表达式，不能加 .over()。"
            )
        # Mutual exclusion: partition_by must not be given both here and via the
        # sequence window's `partition_by=` kwarg. For rolling(n).agg() the kwarg
        # lives on the inner rolling() call.
        if partition_by is not None:
            kwarg_host = (
                self.on
                if self.func in _ROLLING_AGG_FUNCS and isinstance(self.on, CallExpr)
                else self
            )
            if "partition_by" in kwarg_host.kwargs:
                raise ValueError(
                    f"{self.func}() 同时收到 .over(partition_by=) 与 partition_by= kwarg；"
                    f"二者互斥，请二选一。"
                )
        # Mirror sort(): `descending` wins when both are supplied, so
        # over(desc=X, descending=Y) and sort(desc=X, descending=Y) agree.
        if descending is not None:
            resolved = descending
        elif desc is not None:
            resolved = desc
        else:
            resolved = False
        return WindowExpr(
            self, partition_by=partition_by, order_by=order_by, descending=resolved
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
        expr: Any,
        partition_by: Any | None = None,
        order_by: Any | None = None,
        descending: bool = False,
    ):
        """Initialize a WindowExpr with expression and window specification."""
        self.expr = expr
        self.partition_by = partition_by
        self.order_by = order_by
        self.descending = descending

    def serialize(self) -> dict[str, Any]:
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
