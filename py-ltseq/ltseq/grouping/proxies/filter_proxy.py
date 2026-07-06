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


class _RawSQL:
    """Wrapper for raw SQL that should not be quoted."""
    __slots__ = ("sql",)

    def __init__(self, sql: str):
        self.sql = sql


class FilterExpr:
    """A captured filter predicate (comparison expression).

    Represents expressions like g.count() > 2, g.first().price == 100, etc.
    Can be converted to a SQL WHERE clause for execution in the Rust engine.
    """

    def __init__(self, left, op: str, right):
        self.left = left
        self.op = op
        self.right = right

    def to_sql(self) -> str:
        """Convert this filter expression to a SQL WHERE clause."""
        left_sql = self._operand_to_sql(self.left)
        if self.op == "NOT":
            return f"NOT ({left_sql})"
        right_sql = self._operand_to_sql(self.right)
        if self.op == "AND":
            return f"({left_sql}) AND ({right_sql})"
        elif self.op == "OR":
            return f"({left_sql}) OR ({right_sql})"
        else:
            return f"{left_sql} {self.op} {right_sql}"

    def _operand_to_sql(self, operand) -> str:
        if isinstance(operand, _RawSQL):
            return operand.sql
        elif isinstance(operand, GroupExpr):
            from ..sql_parsing import group_expr_to_sql

            return group_expr_to_sql(operand.serialize())
        elif isinstance(operand, FilterExpr):
            return operand.to_sql()
        elif isinstance(operand, str):
            escaped = operand.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(operand, bool):
            return "TRUE" if operand else "FALSE"
        elif operand is None:
            return "NULL"
        else:
            return str(operand)

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

    def count(self) -> GroupCountExpr:
        """Capture g.count() expression."""
        return _FilterableGroupCountExpr()

    def first(self) -> DeriveRowProxy:
        """Capture g.first() - returns proxy for column access."""
        return _FilterableDeriveRowProxy("first")

    def last(self) -> DeriveRowProxy:
        """Capture g.last() - returns proxy for column access."""
        return _FilterableDeriveRowProxy("last")

    def max(self, column: str) -> GroupAggExpr:
        """Capture g.max('column') expression."""
        return _FilterableGroupAggExpr("max", column)

    def min(self, column: str) -> GroupAggExpr:
        """Capture g.min('column') expression."""
        return _FilterableGroupAggExpr("min", column)

    def sum(self, column: str) -> GroupAggExpr:
        """Capture g.sum('column') expression."""
        return _FilterableGroupAggExpr("sum", column)

    def avg(self, column: str) -> GroupAggExpr:
        """Capture g.avg('column') expression."""
        return _FilterableGroupAggExpr("avg", column)

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


class _InnerColumnProxy:
    """Proxy for capturing column references in inner predicates.

    When passed to a lambda like lambda r: r.value > 0, this proxy
    captures r.value as a column name and the comparison as SQL.
    """

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return _InnerColumnRef(name)

    def __gt__(self, other):
        raise TypeError("Compare column attributes, not the row proxy itself")

    def __lt__(self, other):
        raise TypeError("Compare column attributes, not the row proxy itself")


class _InnerColumnRef:
    """A reference to a column captured from the inner predicate proxy."""

    def __init__(self, column: str):
        self._column = column

    def __gt__(self, other):
        return _InnerPredicateExpr(self, ">", other)

    def __ge__(self, other):
        return _InnerPredicateExpr(self, ">=", other)

    def __lt__(self, other):
        return _InnerPredicateExpr(self, "<", other)

    def __le__(self, other):
        return _InnerPredicateExpr(self, "<=", other)

    def __eq__(self, other) -> Any:
        return _InnerPredicateExpr(self, "=", other)

    def __ne__(self, other) -> Any:
        return _InnerPredicateExpr(self, "!=", other)


class _InnerPredicateExpr:
    """A captured inner predicate expression like 'value > 0'."""

    def __init__(self, left, op: str, right=None):
        self.left = left
        self.op = op
        self.right = right

    def to_sql(self) -> str:
        left_sql = self._operand_to_sql(self.left)
        right_sql = self._operand_to_sql(self.right) if self.right is not None else None
        if self.op == "AND":
            return f"({left_sql}) AND ({right_sql})"
        elif self.op == "OR":
            return f"({left_sql}) OR ({right_sql})"
        else:
            return f"{left_sql} {self.op} {right_sql}"

    def _operand_to_sql(self, operand) -> str:
        if isinstance(operand, _InnerPredicateExpr):
            return operand.to_sql()
        elif isinstance(operand, _InnerColumnRef):
            return operand._column
        elif isinstance(operand, str):
            escaped = operand.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(operand, bool):
            return "TRUE" if operand else "FALSE"
        elif operand is None:
            return "NULL"
        else:
            return str(operand)

    def __and__(self, other: "_InnerPredicateExpr") -> "_InnerPredicateExpr":
        return _InnerPredicateExpr(self, "AND", other)

    def __or__(self, other: "_InnerPredicateExpr") -> "_InnerPredicateExpr":
        return _InnerPredicateExpr(self, "OR", other)


def _capture_inner_predicate_proxy(predicate: Callable) -> str | None:
    """Try to capture an inner predicate using a proxy object.

    Passes a _InnerColumnProxy to the predicate lambda, so that
    lambda r: r.value > 0 becomes "value > 0" as SQL.
    """
    try:
        proxy = _InnerColumnProxy()
        result = predicate(proxy)
        if isinstance(result, _InnerPredicateExpr):
            return result.to_sql()
    except Exception:
        pass
    return None


def _capture_inner_predicate_source(predicate: Callable) -> str | None:
    """Try to capture an inner predicate lambda (r.col op val) as SQL.

    Uses source code inspection and the FilterSQLParser's inner predicate logic.
    Falls back to this when proxy-based capture fails (e.g. for complex expressions).
    """
    import ast
    from ..sql_parsing import FilterSQLParser, extract_lambda_from_chain

    parser = FilterSQLParser()

    try:
        import inspect
        source = inspect.getsource(predicate).strip()
    except (OSError, TypeError):
        return None

    if "lambda" in source:
        source = extract_lambda_from_chain(source)
        colon_idx = source.index(":")
        body = source[colon_idx + 1:].strip()
    else:
        body = source
        if body.startswith("return "):
            body = body[7:].strip()

    try:
        tree = ast.parse(body, mode="eval")
        return parser._ast_inner_predicate_to_sql(tree.body)
    except Exception:
        return None


# Filterable variants that support comparison operators

class _FilterableGroupCountExpr(GroupCountExpr, _ComparableGroupExpr):
    """GroupCountExpr that also supports comparison operators for filter capture."""
    pass


class _FilterableGroupAggExpr(GroupAggExpr, _ComparableGroupExpr):
    """GroupAggExpr that also supports comparison operators for filter capture."""

    def __init__(self, func: str, column: str):
        super().__init__(func, column)


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
