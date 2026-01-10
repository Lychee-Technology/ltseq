"""String and Temporal accessor classes for column operations."""

from typing import TYPE_CHECKING

from .base import Expr

if TYPE_CHECKING:
    from .types import CallExpr


class StringAccessor:
    """
    Accessor for string operations on columns.

    Accessed via r.column.s and provides methods like:
    - r.col.s.contains("substring")
    - r.col.s.starts_with("prefix")
    - r.col.s.lower()

    Supports chaining: r.col.s.lower().s.contains("x")
    """

    def __init__(self, expr: Expr):
        """Accept any Expr (ColumnExpr, CallExpr, etc.) for chaining support."""
        self._expr = expr

    def _make_call(self, func: str, args: tuple = ()) -> "CallExpr":
        """Create a CallExpr for this accessor method."""
        from .types import CallExpr

        return CallExpr(func, args, {}, on=self._expr)

    def contains(self, pattern: str) -> "CallExpr":
        """Check if string contains a substring."""
        return self._make_call("str_contains", (pattern,))

    def starts_with(self, prefix: str) -> "CallExpr":
        """Check if string starts with a prefix."""
        return self._make_call("str_starts_with", (prefix,))

    def ends_with(self, suffix: str) -> "CallExpr":
        """Check if string ends with a suffix."""
        return self._make_call("str_ends_with", (suffix,))

    def lower(self) -> "CallExpr":
        """Convert string to lowercase."""
        return self._make_call("str_lower")

    def upper(self) -> "CallExpr":
        """Convert string to uppercase."""
        return self._make_call("str_upper")

    def strip(self) -> "CallExpr":
        """Remove leading and trailing whitespace."""
        return self._make_call("str_strip")

    def len(self) -> "CallExpr":
        """Get string length."""
        return self._make_call("str_len")

    def slice(self, start: int, length: int) -> "CallExpr":
        """Extract substring: start position and length."""
        return self._make_call("str_slice", (start, length))

    def regex_match(self, pattern: str) -> "CallExpr":
        """Check if string matches a regular expression pattern (returns boolean).

        Args:
            pattern: Regular expression pattern to match against

        Returns:
            Boolean expression that is True when pattern matches

        Example:
            >>> t.filter(lambda r: r.email.s.regex_match(r'^[a-z]+@'))
        """
        return self._make_call("str_regex_match", (pattern,))


class TemporalAccessor:
    """
    Accessor for temporal operations on date/datetime columns.

    Accessed via r.column.dt and provides methods like:
    - r.col.dt.year()
    - r.col.dt.month()
    - r.col.dt.add_days(30)
    """

    def __init__(self, expr: Expr):
        """Accept any Expr (ColumnExpr, CallExpr, etc.) for chaining support."""
        self._expr = expr

    def _make_call(self, func: str, args: tuple = ()) -> "CallExpr":
        """Create a CallExpr for this accessor method."""
        from .types import CallExpr

        return CallExpr(func, args, {}, on=self._expr)

    def year(self) -> "CallExpr":
        """Extract year from date/datetime."""
        return self._make_call("dt_year")

    def month(self) -> "CallExpr":
        """Extract month from date/datetime."""
        return self._make_call("dt_month")

    def day(self) -> "CallExpr":
        """Extract day from date/datetime."""
        return self._make_call("dt_day")

    def hour(self) -> "CallExpr":
        """Extract hour from datetime."""
        return self._make_call("dt_hour")

    def minute(self) -> "CallExpr":
        """Extract minute from datetime."""
        return self._make_call("dt_minute")

    def second(self) -> "CallExpr":
        """Extract second from datetime."""
        return self._make_call("dt_second")

    def add(self, days: int = 0, months: int = 0, years: int = 0) -> "CallExpr":
        """Add days, months, and/or years to a date/datetime."""
        return self._make_call("dt_add", (days, months, years))

    def diff(self, other: "Expr") -> "CallExpr":
        """Calculate difference between two dates in days."""
        from .types import CallExpr

        other_coerced = Expr._coerce(other)
        return CallExpr("dt_diff", (other_coerced,), {}, on=self._expr)
