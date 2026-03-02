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

    def replace(self, old: str, new: str) -> "CallExpr":
        """Replace all occurrences of a substring with another string.

        Args:
            old: The substring to search for
            new: The replacement string

        Returns:
            Expression with all occurrences of 'old' replaced with 'new'

        Example:
            >>> t.derive(clean_name=lambda r: r.name.s.replace("-", "_"))
        """
        return self._make_call("str_replace", (old, new))

    def concat(self, *others) -> "CallExpr":
        """Concatenate this string with other strings.

        Args:
            *others: Additional strings or column expressions to concatenate

        Returns:
            Expression representing the concatenated string

        Example:
            >>> t.derive(full_name=lambda r: r.first.s.concat(" ", r.last))
        """
        return self._make_call("str_concat", others)

    def pad_left(self, width: int, char: str = " ") -> "CallExpr":
        """Pad string on the left to reach specified width.

        Args:
            width: Target width of the resulting string
            char: Character to use for padding (default: space)

        Returns:
            Left-padded string expression

        Example:
            >>> t.derive(padded_id=lambda r: r.id.s.pad_left(5, "0"))
        """
        return self._make_call("str_pad_left", (width, char))

    def pad_right(self, width: int, char: str = " ") -> "CallExpr":
        """Pad string on the right to reach specified width.

        Args:
            width: Target width of the resulting string
            char: Character to use for padding (default: space)

        Returns:
            Right-padded string expression

        Example:
            >>> t.derive(padded_name=lambda r: r.name.s.pad_right(20, "."))
        """
        return self._make_call("str_pad_right", (width, char))

    def split(self, delimiter: str, index: int) -> "CallExpr":
        """Split string by delimiter and return the part at specified index.

        Args:
            delimiter: String to split on
            index: 1-based index of the part to return (1 = first part)

        Returns:
            Expression representing the specified part of the split string

        Note:
            Index is 1-based to match SQL SPLIT_PART convention.
            Returns empty string if index is out of range.

        Example:
            >>> # Get domain from email "user@example.com"
            >>> t.derive(domain=lambda r: r.email.s.split("@", 2))
        """
        return self._make_call("str_split", (delimiter, index))

    def like(self, pattern: str) -> "CallExpr":
        """SQL LIKE pattern matching (% matches any sequence, _ matches one char).

        Example:
            >>> t.filter(lambda r: r.name.s.like("J%"))
        """
        return self._make_call("str_like", (pattern,))

    def isalpha(self) -> "CallExpr":
        """Return True if all characters in the string are alphabetic.

        Example:
            >>> t.filter(lambda r: r.code.s.isalpha())
        """
        return self._make_call("str_isalpha")

    def isdigit(self) -> "CallExpr":
        """Return True if all characters in the string are digits.

        Example:
            >>> t.filter(lambda r: r.zip_code.s.isdigit())
        """
        return self._make_call("str_isdigit")

    def islower(self) -> "CallExpr":
        """Return True if all cased characters are lowercase.

        Example:
            >>> t.filter(lambda r: r.tag.s.islower())
        """
        return self._make_call("str_islower")

    def isupper(self) -> "CallExpr":
        """Return True if all cased characters are uppercase.

        Example:
            >>> t.filter(lambda r: r.code.s.isupper())
        """
        return self._make_call("str_isupper")

    def pos(self, sub: str) -> "CallExpr":
        """Find 1-based position of substring (0 if not found). Equivalent to SQL STRPOS.

        Args:
            sub: Substring to search for

        Returns:
            Integer expression: 1-based position, or 0 if not found

        Example:
            >>> t.derive(at_pos=lambda r: r.email.s.pos("@"))
        """
        return self._make_call("str_pos", (sub,))

    def left(self, n: int) -> "CallExpr":
        """Return the leftmost n characters of the string.

        Args:
            n: Number of characters to extract from the left

        Returns:
            String expression with the first n characters

        Example:
            >>> t.derive(prefix=lambda r: r.code.s.left(3))
        """
        return self._make_call("str_left", (n,))

    def right(self, n: int) -> "CallExpr":
        """Return the rightmost n characters of the string.

        Args:
            n: Number of characters to extract from the right

        Returns:
            String expression with the last n characters

        Example:
            >>> t.derive(suffix=lambda r: r.code.s.right(4))
        """
        return self._make_call("str_right", (n,))

    def lstrip(self) -> "CallExpr":
        """Remove leading (left) whitespace from the string.

        Returns:
            String expression with leading whitespace removed

        Example:
            >>> t.derive(clean=lambda r: r.name.s.lstrip())
        """
        return self._make_call("str_ltrim")

    def rstrip(self) -> "CallExpr":
        """Remove trailing (right) whitespace from the string.

        Returns:
            String expression with trailing whitespace removed

        Example:
            >>> t.derive(clean=lambda r: r.name.s.rstrip())
        """
        return self._make_call("str_rtrim")

    def asc(self) -> "CallExpr":
        """Return the Unicode code point of the first character. Equivalent to ord(s[0]).

        Returns:
            Integer expression with the ASCII/Unicode code of the first character

        Example:
            >>> t.derive(code=lambda r: r.initial.s.asc())
        """
        return self._make_call("str_asc")


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

    def add(
        self,
        days: int = 0,
        months: int = 0,
        years: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        weeks: int = 0,
    ) -> "CallExpr":
        """Add a duration to a date/datetime.

        Args:
            days: Number of days to add (negative to subtract)
            months: Number of months to add
            years: Number of years to add
            hours: Number of hours to add
            minutes: Number of minutes to add
            seconds: Number of seconds to add
            weeks: Number of weeks to add (converted to days × 7)

        Example:
            >>> t.derive(next_month=lambda r: r.date.dt.add(months=1))
            >>> t.derive(in_2h=lambda r: r.ts.dt.add(hours=2))
        """
        return self._make_call("dt_add", (days, months, years, hours, minutes, seconds, weeks))

    def diff(self, other: "Expr", unit: str = "day") -> "CallExpr":
        """Calculate the difference between two dates/datetimes.

        Args:
            other: The other date/datetime to subtract
            unit: Unit of the result — one of "day", "month", "year",
                  "hour", "minute", "second" (default: "day")

        Returns:
            Integer expression representing the difference in the specified unit

        Example:
            >>> t.derive(days_open=lambda r: r.close_date.dt.diff(r.open_date))
            >>> t.derive(months_old=lambda r: r.today.dt.diff(r.birth_date, unit="month"))
        """
        from .types import CallExpr, LiteralExpr

        other_coerced = Expr._coerce(other)
        return CallExpr("dt_diff", (other_coerced, LiteralExpr(unit)), {}, on=self._expr)

    def age(self) -> "CallExpr":
        """Return the number of complete years between this date and today.

        Equivalent to SPL's age(dt): the number of full years elapsed since the date.

        Returns:
            Integer expression with the age in complete years

        Example:
            >>> t.derive(age_years=lambda r: r.birth_date.dt.age())
        """
        return self._make_call("dt_age")

    def millisecond(self) -> "CallExpr":
        """Extract millisecond component (0–999) from a timestamp.

        Example:
            >>> t.derive(ms=lambda r: r.ts.dt.millisecond())
        """
        return self._make_call("dt_millisecond")

    def weekday(self) -> "CallExpr":
        """Extract weekday as 0-based integer (Monday=0 … Sunday=6).

        Example:
            >>> t.derive(wd=lambda r: r.date.dt.weekday())
        """
        return self._make_call("dt_weekday")
