"""FilterGroupProxy - Proxy class for capturing filter predicates.

Captures group-level filter expressions like g.count() > 2 as serializable
expression trees, enabling proxy-based filter capture that works in all contexts
(pytest, REPL, exec) without requiring source code inspection.
"""

from typing import Any, Callable

from ..expr import (
    GroupExpr,
    GroupCountExpr,
    GroupAggExpr,
    GroupRowColumnExpr,
    DeriveRowProxy,
)


class FilterExpr:
    """A captured filter predicate (comparison expression).

    Represents expressions like g.count() > 2, g.first().price == 100, etc.
    Can be converted to a SQL WHERE clause for execution in the Rust engine.
    """

    def __init__(self, left, op: str, right):
        self.left = left
        self.op = op
        self.right = right

    # Comparison / combinator symbols → row-dialect op names (shared
    # operator table on the Rust side).
    _OP_NAMES = {
        ">": "Gt", ">=": "Ge", "<": "Lt", "<=": "Le", "=": "Eq", "!=": "Ne",
        "AND": "And", "OR": "Or",
        "+": "Add", "-": "Sub", "*": "Mul", "/": "Div",
    }

    def serialize(self) -> dict:
        """Serialize to the group dialect consumed by Rust filter_group_window."""
        if self.op == "NOT":
            return {
                "type": "UnaryOp",
                "op": "Not",
                "operand": self._serialize_operand(self.left),
            }
        return {
            "type": "BinOp",
            "op": self._OP_NAMES.get(self.op, self.op),
            "left": self._serialize_operand(self.left),
            "right": self._serialize_operand(self.right),
        }

    @staticmethod
    def _serialize_operand(operand) -> dict:
        if isinstance(operand, (FilterExpr, GroupExpr)):
            return operand.serialize()
        from ...expr import LiteralExpr

        return LiteralExpr(operand).serialize()

    def __and__(self, other: "FilterExpr") -> "FilterExpr":
        """Combine two filter expressions with AND."""
        return FilterExpr(self, "AND", other)

    def __or__(self, other: "FilterExpr") -> "FilterExpr":
        """Combine two filter expressions with OR."""
        return FilterExpr(self, "OR", other)

    def __invert__(self) -> "FilterExpr":
        """Negate a filter expression with NOT."""
        return FilterExpr(self, "NOT", None)


class _ComparableGroupExpr(GroupExpr):
    """Mixin-like base that adds comparison operators to GroupExpr subclasses.

    When a comparison like g.count() > 2 is evaluated, Python calls __gt__
    which returns a FilterExpr capturing the operation.
    """

    def __gt__(self, other) -> FilterExpr:
        return FilterExpr(self, ">", other)

    def __ge__(self, other) -> FilterExpr:
        return FilterExpr(self, ">=", other)

    def __lt__(self, other) -> FilterExpr:
        return FilterExpr(self, "<", other)

    def __le__(self, other) -> FilterExpr:
        return FilterExpr(self, "<=", other)

    def __eq__(self, other) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self, "=", other)

    def __ne__(self, other) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self, "!=", other)


class QuantifierFilterExpr(FilterExpr):
    """A captured quantifier: g.all/any/none(lambda r: <row predicate>).

    The inner predicate is a full row-dialect expression dict (captured via
    _lambda_to_expr with schema validation); Rust plans it as
    MIN/MAX(CASE WHEN pred THEN 1 ELSE 0) OVER (PARTITION BY __group_id__).
    """

    def __init__(self, quant: str, pred_dict: dict):
        super().__init__(None, "QUANT", None)
        self._quant = quant
        self._pred_dict = pred_dict

    def serialize(self) -> dict:
        return {"type": "GroupQuantifier", "quant": self._quant, "pred": self._pred_dict}


class FilterGroupProxy:
    """Proxy for capturing filter predicates without executing them.

    When passed to a filter lambda, this proxy captures operations like
    g.count() > 2 as FilterExpr objects that serialize to the group dialect.

    Example:
        proxy = FilterGroupProxy(schema)
        result = filter_lambda(proxy)
        # result is a FilterExpr
        expr_dict = result.serialize()
    """

    def __init__(self, schema: "dict[str, str] | None" = None):
        # Schema of the source table, used to validate and capture the inner
        # row-level lambdas of quantifiers (g.all/any/none).
        self._schema = schema

    def count(self) -> "_FilterableGroupCountExpr":
        """Capture g.count() expression."""
        return _FilterableGroupCountExpr()

    def first(self) -> "_FilterableDeriveRowProxy":
        """Capture g.first() - returns proxy for column access."""
        return _FilterableDeriveRowProxy("first")

    def last(self) -> "_FilterableDeriveRowProxy":
        """Capture g.last() - returns proxy for column access."""
        return _FilterableDeriveRowProxy("last")

    def max(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.max('column') expression."""
        return _FilterableGroupAggExpr("max", column)

    def min(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.min('column') expression."""
        return _FilterableGroupAggExpr("min", column)

    def sum(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.sum('column') expression."""
        return _FilterableGroupAggExpr("sum", column)

    def avg(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.avg('column') expression."""
        return _FilterableGroupAggExpr("avg", column)

    def mean(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.mean('column') expression (alias for avg, Pandas/Polars verb)."""
        return _FilterableGroupAggExpr("avg", column)

    def median(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.median('column') expression."""
        return _FilterableGroupAggExpr("median", column)

    def std(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.std('column') expression (sample standard deviation)."""
        return _FilterableGroupAggExpr("std", column)

    def var(self, column: str) -> "_FilterableGroupAggExpr":
        """Capture g.var('column') expression (sample variance)."""
        return _FilterableGroupAggExpr("var", column)

    def percentile(self, column: str, p: float) -> "_FilterableGroupAggExpr":
        """Capture g.percentile('column', p) expression (approximate, p in [0, 1])."""
        return _FilterableGroupAggExpr("percentile", column, arg=p)

    def all(self, predicate: Callable) -> FilterExpr:
        """Capture g.all(lambda r: ...) as a quantifier over a row-dialect
        predicate. The lambda gets the full row expression surface (schema
        validated at capture time)."""
        return QuantifierFilterExpr("all", self._capture_inner(predicate, "all"))

    def any(self, predicate: Callable) -> FilterExpr:
        """Capture g.any(lambda r: ...) as a quantifier over a row-dialect
        predicate. The lambda gets the full row expression surface (schema
        validated at capture time)."""
        return QuantifierFilterExpr("any", self._capture_inner(predicate, "any"))

    def none(self, predicate: Callable) -> FilterExpr:
        """Capture g.none(lambda r: ...) as a quantifier over a row-dialect
        predicate. The lambda gets the full row expression surface (schema
        validated at capture time)."""
        return QuantifierFilterExpr("none", self._capture_inner(predicate, "none"))

    def _capture_inner(self, predicate: Callable, which: str) -> dict:
        if self._schema is None:
            raise ValueError(
                f"g.{which}() requires the group proxy to know the table schema; "
                "construct FilterGroupProxy(schema)"
            )
        from ...expr import _lambda_to_expr

        return _lambda_to_expr(predicate, self._schema)


# Filterable variants that support comparison operators

class _FilterableGroupCountExpr(GroupCountExpr, _ComparableGroupExpr):
    """GroupCountExpr that also supports comparison operators for filter capture."""
    pass


class _FilterableGroupAggExpr(GroupAggExpr, _ComparableGroupExpr):
    """GroupAggExpr that also supports comparison operators for filter capture."""

    def __init__(self, func: str, column: str, arg: "float | None" = None):
        super().__init__(func, column, arg)


class _FilterableGroupRowColumnExpr(GroupRowColumnExpr, _ComparableGroupExpr):
    """GroupRowColumnExpr that also supports comparison operators for filter capture."""

    def __init__(self, row_type: str, column: str):
        super().__init__(row_type, column)


class _FilterableDeriveRowProxy(DeriveRowProxy):
    """DeriveRowProxy that returns filterable column expressions."""

    def __init__(self, row_type: str):
        super().__init__(row_type)

    def __getattr__(self, name: str) -> _FilterableGroupRowColumnExpr:
        if name.startswith("_"):
            raise AttributeError(name)
        return _FilterableGroupRowColumnExpr(self._row_type, name)
