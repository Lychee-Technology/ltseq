"""Base expression class for LTSeq."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union


def if_else(condition: "Expr", true_value: Any, false_value: Any) -> "CallExpr":
    """
    Conditional expression: returns true_value if condition else false_value.

    Equivalent to SQL CASE WHEN condition THEN true_value ELSE false_value END.

    Args:
        condition: A boolean expression to evaluate
        true_value: Value to return if condition is True
        false_value: Value to return if condition is False

    Returns:
        Expression that evaluates to one of the two values

    Example:
        >>> t.derive(lambda r: {"category": if_else(r.price > 100, "expensive", "cheap")})
    """
    from .types import CallExpr

    return CallExpr(
        "if_else",
        (condition, Expr._coerce(true_value), Expr._coerce(false_value)),
        {},
        on=None,
    )


def count_if(predicate: "Expr") -> "CallExpr":
    """
    Count rows where predicate is True.

    Args:
        predicate: A boolean expression to evaluate

    Returns:
        Expression that counts matching rows

    Example:
        >>> t.agg(by=lambda r: r.region, high_value=lambda g: count_if(g.price > 100))
    """
    from .types import CallExpr

    return CallExpr("count_if", (predicate,), {}, on=None)


def sum_if(predicate: "Expr", column: "Expr") -> "CallExpr":
    """
    Sum column values where predicate is True.

    Args:
        predicate: A boolean expression to evaluate
        column: The column to sum

    Returns:
        Expression that sums matching values

    Example:
        >>> t.agg(by=lambda r: r.region, high_sales=lambda g: sum_if(g.price > 100, g.sales))
    """
    from .types import CallExpr

    return CallExpr("sum_if", (predicate, column), {}, on=None)


def avg_if(predicate: "Expr", column: "Expr") -> "CallExpr":
    """
    Average column values where predicate is True.

    Args:
        predicate: A boolean expression to evaluate
        column: The column to average

    Returns:
        Expression that averages matching values

    Example:
        >>> t.agg(by=lambda r: r.region, avg_high=lambda g: avg_if(g.price > 100, g.sales))
    """
    from .types import CallExpr

    return CallExpr("avg_if", (predicate, column), {}, on=None)


def min_if(predicate: "Expr", column: "Expr") -> "CallExpr":
    """
    Minimum column value where predicate is True.

    Args:
        predicate: A boolean expression to evaluate
        column: The column to find minimum

    Returns:
        Expression that returns minimum of matching values

    Example:
        >>> t.agg(by=lambda r: r.region, min_active=lambda g: min_if(g.is_active, g.score))
    """
    from .types import CallExpr

    return CallExpr("min_if", (predicate, column), {}, on=None)


def max_if(predicate: "Expr", column: "Expr") -> "CallExpr":
    """
    Maximum column value where predicate is True.

    Args:
        predicate: A boolean expression to evaluate
        column: The column to find maximum

    Returns:
        Expression that returns maximum of matching values

    Example:
        >>> t.agg(by=lambda r: r.region, max_active=lambda g: max_if(g.is_active, g.score))
    """
    from .types import CallExpr

    return CallExpr("max_if", (predicate, column), {}, on=None)


def coalesce(*args: "Expr") -> "CallExpr":
    """
    Return the first non-null value from the arguments.

    Equivalent to SQL COALESCE(a, b, c, ...).

    Args:
        *args: Two or more expressions or values. Returns the first non-null.

    Returns:
        Expression that evaluates to the first non-null argument

    Example:
        >>> from ltseq import coalesce
        >>> t.derive(name=lambda r: coalesce(r.preferred_name, r.full_name, "Unknown"))
    """
    from .types import CallExpr

    coerced = tuple(Expr._coerce(a) for a in args)
    return CallExpr("coalesce", coerced, {}, on=None)


# =============================================================================
# Ranking / Window Functions
# =============================================================================


def row_number() -> "CallExpr":
    """
    Assign a unique sequential number to each row starting from 1.

    Unlike rank() and dense_rank(), row_number() always produces distinct values
    even for rows with identical ordering values.

    Must be used with .over() to specify ordering.

    Returns:
        Expression representing the row number

    Example:
        >>> t.derive(lambda r: {"rn": row_number().over(order_by=r.date)})
        >>> t.derive(lambda r: {"rn": row_number().over(partition_by=r.group, order_by=r.date)})
    """
    from .types import CallExpr

    return CallExpr("row_number", (), {}, on=None)


def rank() -> "CallExpr":
    """
    Assign a rank to each row, with gaps for ties.

    Rows with equal values get the same rank, and the next rank is skipped.
    For example: 1, 2, 2, 4, 5 (rank 3 is skipped because there are two rank 2s).

    Must be used with .over() to specify ordering.

    Returns:
        Expression representing the rank

    Example:
        >>> t.derive(lambda r: {"rank": rank().over(order_by=r.score)})
        >>> t.derive(lambda r: {"rank": rank().over(partition_by=r.dept, order_by=r.salary)})
    """
    from .types import CallExpr

    return CallExpr("rank", (), {}, on=None)


def dense_rank() -> "CallExpr":
    """
    Assign a rank to each row, without gaps for ties.

    Rows with equal values get the same rank, but the next rank is not skipped.
    For example: 1, 2, 2, 3, 4 (no gap after the tied rank 2s).

    Must be used with .over() to specify ordering.

    Returns:
        Expression representing the dense rank

    Example:
        >>> t.derive(lambda r: {"drank": dense_rank().over(order_by=r.score)})
        >>> t.derive(lambda r: {"drank": dense_rank().over(partition_by=r.dept, order_by=r.salary)})
    """
    from .types import CallExpr

    return CallExpr("dense_rank", (), {}, on=None)


def ntile(n: int) -> "CallExpr":
    """
    Divide rows into n roughly equal buckets (1 to n).

    Useful for creating quantiles, percentiles, or distributing rows evenly.

    Must be used with .over() to specify ordering.

    Args:
        n: Number of buckets to divide into

    Returns:
        Expression representing the bucket number (1 to n)

    Example:
        >>> t.derive(lambda r: {"quartile": ntile(4).over(order_by=r.score)})
        >>> t.derive(lambda r: {"decile": ntile(10).over(partition_by=r.group, order_by=r.value)})
    """
    from .types import CallExpr

    return CallExpr("ntile", (n,), {}, on=None)


class Expr(ABC):
    """
    Abstract base class for all expression types.

    Implements magic methods (__add__, __gt__, etc.) that return new Expr objects
    instead of evaluating. This allows building expression trees from Python code.
    """

    @abstractmethod
    def serialize(self) -> Dict[str, Any]:
        """
        Convert this expression to a serializable nested dict.

        Returns:
            A dict with at least a 'type' key, ready for Rust deserialization.
        """
        pass

    # Arithmetic operators
    def __add__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Addition operator: expr + other"""
        from .types import BinOpExpr

        return BinOpExpr("Add", self, self._coerce(other))

    def __sub__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Subtraction operator: expr - other"""
        from .types import BinOpExpr

        return BinOpExpr("Sub", self, self._coerce(other))

    def __mul__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Multiplication operator: expr * other"""
        from .types import BinOpExpr

        return BinOpExpr("Mul", self, self._coerce(other))

    def __truediv__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Division operator: expr / other"""
        from .types import BinOpExpr

        return BinOpExpr("Div", self, self._coerce(other))

    def __floordiv__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Floor division operator: expr // other"""
        from .types import BinOpExpr

        return BinOpExpr("FloorDiv", self, self._coerce(other))

    def __mod__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Modulo operator: expr % other"""
        from .types import BinOpExpr

        return BinOpExpr("Mod", self, self._coerce(other))

    # Comparison operators
    # Note: These override object.__eq__ and __ne__, intentionally returning Expr instead of bool
    def __eq__(self, other: Union["Expr", Any]):  # type: ignore
        """Equality operator: expr == other"""
        from .types import BinOpExpr

        return BinOpExpr("Eq", self, self._coerce(other))

    def __ne__(self, other: Union["Expr", Any]):  # type: ignore
        """Inequality operator: expr != other"""
        from .types import BinOpExpr

        return BinOpExpr("Ne", self, self._coerce(other))

    def __lt__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Less than operator: expr < other"""
        from .types import BinOpExpr

        return BinOpExpr("Lt", self, self._coerce(other))

    def __le__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Less than or equal operator: expr <= other"""
        from .types import BinOpExpr

        return BinOpExpr("Le", self, self._coerce(other))

    def __gt__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Greater than operator: expr > other"""
        from .types import BinOpExpr

        return BinOpExpr("Gt", self, self._coerce(other))

    def __ge__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Greater than or equal operator: expr >= other"""
        from .types import BinOpExpr

        return BinOpExpr("Ge", self, self._coerce(other))

    # Logical operators
    def __and__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Logical AND operator: expr & other"""
        from .types import BinOpExpr

        return BinOpExpr("And", self, self._coerce(other))

    def __or__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Logical OR operator: expr | other"""
        from .types import BinOpExpr

        return BinOpExpr("Or", self, self._coerce(other))

    def __invert__(self) -> "UnaryOpExpr":
        """Logical NOT operator: ~expr"""
        from .types import UnaryOpExpr

        return UnaryOpExpr("Not", self)

    def __abs__(self) -> "CallExpr":
        """Absolute value operator: abs(expr)"""
        from .types import CallExpr

        return CallExpr("abs", (self,), {}, on=None)

    # Right-hand operators (for reversed operations like 5 + r.col)
    def __radd__(self, other: Any) -> "BinOpExpr":
        """Right addition: other + expr"""
        from .types import BinOpExpr

        return BinOpExpr("Add", self._coerce(other), self)

    def __rsub__(self, other: Any) -> "BinOpExpr":
        """Right subtraction: other - expr"""
        from .types import BinOpExpr

        return BinOpExpr("Sub", self._coerce(other), self)

    def __rmul__(self, other: Any) -> "BinOpExpr":
        """Right multiplication: other * expr"""
        from .types import BinOpExpr

        return BinOpExpr("Mul", self._coerce(other), self)

    def __rtruediv__(self, other: Any) -> "BinOpExpr":
        """Right division: other / expr"""
        from .types import BinOpExpr

        return BinOpExpr("Div", self._coerce(other), self)

    def __rfloordiv__(self, other: Any) -> "BinOpExpr":
        """Right floor division: other // expr"""
        from .types import BinOpExpr

        return BinOpExpr("FloorDiv", self._coerce(other), self)

    def __rmod__(self, other: Any) -> "BinOpExpr":
        """Right modulo: other % expr"""
        from .types import BinOpExpr

        return BinOpExpr("Mod", self._coerce(other), self)

    def fill_null(self, default: Any) -> "CallExpr":
        """
        Replace null values with a default value.

        Args:
            default: The value to use when column is null

        Returns:
            Expression that replaces nulls with default

        Example:
            >>> t.derive(lambda r: {"email": r.email.fill_null("unknown@example.com")})
        """
        from .types import CallExpr

        return CallExpr("fill_null", (self._coerce(default),), {}, on=self)

    def is_null(self) -> "CallExpr":
        """
        Check if the value is null.

        Returns:
            Boolean expression that is True when value is null

        Example:
            >>> t.filter(lambda r: r.email.is_null())
        """
        from .types import CallExpr

        return CallExpr("is_null", (), {}, on=self)

    def is_not_null(self) -> "CallExpr":
        """
        Check if the value is not null.

        Returns:
            Boolean expression that is True when value is not null

        Example:
            >>> t.filter(lambda r: r.email.is_not_null())
        """
        from .types import CallExpr

        return CallExpr("is_not_null", (), {}, on=self)

    def is_in(self, values: list) -> "CallExpr":
        """
        Check if the value is contained in a list of values.

        Equivalent to SQL: column IN (val1, val2, val3)

        Args:
            values: A list of values to check membership against

        Returns:
            Boolean expression that is True when value is in the list

        Example:
            >>> t.filter(lambda r: r.status.is_in(["active", "pending", "review"]))
            >>> t.filter(lambda r: r.age.is_in([18, 21, 65]))
        """
        from .types import CallExpr

        coerced_values = [self._coerce(v) for v in values]
        return CallExpr("is_in", tuple(coerced_values), {}, on=self)

    def between(self, low: Any, high: Any) -> "BinOpExpr":
        """
        Check if the value is between low and high (inclusive).

        Equivalent to: (expr >= low) & (expr <= high)

        Args:
            low: Lower bound (inclusive)
            high: Upper bound (inclusive)

        Returns:
            Boolean expression that is True when value is in range

        Example:
            >>> t.filter(lambda r: r.price.between(10, 100))
            >>> t.filter(lambda r: r.age.between(18, 65))
        """
        return (self >= self._coerce(low)) & (self <= self._coerce(high))

    def cast(self, dtype: str) -> "CallExpr":
        """
        Cast the expression to a different data type.

        Supported types: "int32", "int64", "float32", "float64",
        "utf8", "string", "bool", "date32", "timestamp"

        Args:
            dtype: Target data type name

        Returns:
            Expression cast to the target type

        Example:
            >>> t.derive(price_str=lambda r: r.price.cast("utf8"))
            >>> t.derive(amount=lambda r: r.amount_str.cast("float64"))
        """
        from .types import CallExpr, LiteralExpr

        return CallExpr("cast", (LiteralExpr(dtype),), {}, on=self)

    def abs(self) -> "CallExpr":
        """
        Compute the absolute value.

        Returns:
            Expression with absolute value applied

        Example:
            >>> t.derive(abs_change=lambda r: (r.price - r.prev_price).abs())
        """
        from .types import CallExpr

        return CallExpr("abs", (), {}, on=self)

    def round(self, decimals: int = 0) -> "CallExpr":
        """
        Round to the specified number of decimal places.

        Args:
            decimals: Number of decimal places (default 0)

        Returns:
            Expression with rounding applied

        Example:
            >>> t.derive(rounded=lambda r: r.score.round(2))
        """
        from .types import CallExpr, LiteralExpr

        return CallExpr("round", (LiteralExpr(decimals),), {}, on=self)

    def floor(self) -> "CallExpr":
        """
        Round down to the nearest integer.

        Returns:
            Expression with floor applied

        Example:
            >>> t.derive(bucket=lambda r: r.value.floor())
        """
        from .types import CallExpr

        return CallExpr("floor", (), {}, on=self)

    def ceil(self) -> "CallExpr":
        """
        Round up to the nearest integer.

        Returns:
            Expression with ceiling applied

        Example:
            >>> t.derive(bucket=lambda r: r.value.ceil())
        """
        from .types import CallExpr

        return CallExpr("ceil", (), {}, on=self)

    def pct_change(self) -> "BinOpExpr":
        """
        Compute the percentage change from the previous row.

        Equivalent to: (value - value.shift(1)) / value.shift(1)

        Requires the table to be sorted. Common in financial analysis.

        Returns:
            Expression representing percentage change from previous row

        Example:
            >>> t.sort("date").derive(daily_return=lambda r: r.close.pct_change())
        """
        from .types import CallExpr

        shifted = CallExpr("shift", (self._coerce(1),), {}, on=self)
        return (self - shifted) / shifted

    @staticmethod
    def _coerce(value: Any) -> "Expr":
        """
        Convert Python literals to LiteralExpr.

        Args:
            value: A Python value or Expr

        Returns:
            An Expr (unchanged if already Expr, wrapped in LiteralExpr otherwise)
        """
        if isinstance(value, Expr):
            return value
        from .types import LiteralExpr

        return LiteralExpr(value)

    def lookup(
        self, target_table: Any, column: str, join_key: Optional[str] = None
    ) -> "LookupExpr":
        """
        Lookup a value in another table using this expression as the join key.

        Used to fetch a single column from a related table in derived expressions.
        Works on any expression type (column reference, method call result, etc.).

        Args:
            target_table: The target table object (LTSeq instance)
            column: Column name to fetch from the target table
            join_key: Column name in target table to join on (optional)

        Returns:
            A LookupExpr that can be used in derive() or other expressions

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> # Simple lookup on column
            >>> enriched = orders.derive(
            ...     product_name=lambda r: r.product_id.lookup(products, "name")
            ... )
            >>> # Lookup on transformed value
            >>> enriched = orders.derive(
            ...     product_name=lambda r: r.product_id.lower().lookup(products, "name")
            ... )
        """
        from .types import LookupExpr

        # Generate a unique target name based on table identity
        target_name = getattr(target_table, "_name", None)
        if target_name is None:
            target_name = f"lookup_table_{id(target_table)}"

        return LookupExpr(
            on=self,
            target_table=target_table,
            target_name=target_name,
            target_columns=[column],
            join_key=join_key,
        )
