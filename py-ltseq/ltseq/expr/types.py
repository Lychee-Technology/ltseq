"""Concrete expression types for LTSeq."""

from typing import Any, Dict, Optional

from .base import Expr


class ColumnExpr(Expr):
    """
    Represents a column reference, e.g., r.age in a lambda.

    When accessed as r.col.shift(1), __getattr__ returns a callable that
    creates a CallExpr when invoked.

    Attributes:
        name (str): The column name
    """

    def __init__(self, name: str):
        """
        Initialize a ColumnExpr.

        Args:
            name: Column name (e.g., "age", "price")
        """
        self.name = name

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict.

        Returns:
            {"type": "Column", "name": self.name}
        """
        return {"type": "Column", "name": self.name}

    def __getattr__(self, method_name: str):
        """
        Handle method calls like r.col.shift(1), r.col.rolling(3), etc.

        Returns a callable that, when invoked with arguments, creates a CallExpr.

        Args:
            method_name: Method to call (e.g., "shift", "rolling", "contains")

        Returns:
            A callable that accepts (*args, **kwargs) and returns CallExpr

        Raises:
            AttributeError: If method_name starts with underscore (private)
        """
        if method_name.startswith("_"):
            # Avoid issues with __dict__, __class__, etc.
            raise AttributeError(f"No attribute {method_name}")

        def method_call(*args, **kwargs):
            """Create a CallExpr when the method is invoked."""
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

    def lookup(
        self, target_table: "Any", column: str, join_key: Optional[str] = None
    ) -> "LookupExpr":
        """
        Lookup a value in another table using this column as the join key.

        Used to fetch a single column from a related table in derived expressions.

        Args:
            target_table: The target table object (LTSeq instance)
            column: Column name to fetch from the target table
            join_key: Column name in target table to join on (optional)

        Returns:
            A LookupExpr that can be used in derive() or other expressions

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> enriched = orders.derive(
            ...     product_name=lambda r: r.product_id.lookup(products, "name")
            ... )
        """
        # If target_table is an LTSeq, we'll store just the table name for now
        # The actual resolution happens during derive
        target_name = getattr(target_table, "_name", "target")

        return LookupExpr(
            on=self,
            target_name=target_name,
            target_columns=[column],
            join_key=join_key,
        )


class StringAccessor:
    """
    Accessor for string operations on columns.

    Accessed via r.column.s and provides methods like:
    - r.col.s.contains("substring")
    - r.col.s.starts_with("prefix")
    - r.col.s.lower()
    """

    def __init__(self, column: ColumnExpr):
        self._column = column

    def contains(self, pattern: str) -> "CallExpr":
        """Check if string contains a substring."""
        return CallExpr("str_contains", (pattern,), {}, on=self._column)

    def starts_with(self, prefix: str) -> "CallExpr":
        """Check if string starts with a prefix."""
        return CallExpr("str_starts_with", (prefix,), {}, on=self._column)

    def ends_with(self, suffix: str) -> "CallExpr":
        """Check if string ends with a suffix."""
        return CallExpr("str_ends_with", (suffix,), {}, on=self._column)

    def lower(self) -> "CallExpr":
        """Convert string to lowercase."""
        return CallExpr("str_lower", (), {}, on=self._column)

    def upper(self) -> "CallExpr":
        """Convert string to uppercase."""
        return CallExpr("str_upper", (), {}, on=self._column)

    def strip(self) -> "CallExpr":
        """Remove leading and trailing whitespace."""
        return CallExpr("str_strip", (), {}, on=self._column)

    def len(self) -> "CallExpr":
        """Get string length."""
        return CallExpr("str_len", (), {}, on=self._column)

    def slice(self, start: int, length: int) -> "CallExpr":
        """Extract substring: start position and length."""
        return CallExpr("str_slice", (start, length), {}, on=self._column)

    def regex_match(self, pattern: str) -> "CallExpr":
        """Check if string matches a regular expression pattern (returns boolean).

        Args:
            pattern: Regular expression pattern to match against

        Returns:
            Boolean expression that is True when pattern matches

        Example:
            >>> t.filter(lambda r: r.email.s.regex_match(r'^[a-z]+@'))
        """
        return CallExpr("str_regex_match", (pattern,), {}, on=self._column)


class TemporalAccessor:
    """
    Accessor for temporal operations on date/datetime columns.

    Accessed via r.column.dt and provides methods like:
    - r.col.dt.year()
    - r.col.dt.month()
    - r.col.dt.add_days(30)
    """

    def __init__(self, column: ColumnExpr):
        self._column = column

    def year(self) -> "CallExpr":
        """Extract year from date/datetime."""
        return CallExpr("dt_year", (), {}, on=self._column)

    def month(self) -> "CallExpr":
        """Extract month from date/datetime."""
        return CallExpr("dt_month", (), {}, on=self._column)

    def day(self) -> "CallExpr":
        """Extract day from date/datetime."""
        return CallExpr("dt_day", (), {}, on=self._column)

    def hour(self) -> "CallExpr":
        """Extract hour from datetime."""
        return CallExpr("dt_hour", (), {}, on=self._column)

    def minute(self) -> "CallExpr":
        """Extract minute from datetime."""
        return CallExpr("dt_minute", (), {}, on=self._column)

    def second(self) -> "CallExpr":
        """Extract second from datetime."""
        return CallExpr("dt_second", (), {}, on=self._column)

    def add(self, days: int = 0, months: int = 0, years: int = 0) -> "CallExpr":
        """Add days, months, and/or years to a date/datetime."""
        return CallExpr("dt_add", (days, months, years), {}, on=self._column)

    def diff(self, other: "Expr") -> "CallExpr":
        """Calculate difference between two dates in days."""
        from .base import Expr as BaseExpr

        other_coerced = BaseExpr._coerce(other)
        return CallExpr("dt_diff", (other_coerced,), {}, on=self._column)


class LiteralExpr(Expr):
    """
    Represents a constant value: int, float, str, bool, None.

    Includes type inference to determine the appropriate Arrow/DataFusion type.

    Attributes:
        value: The Python value
    """

    def __init__(self, value: Any):
        """
        Initialize a LiteralExpr.

        Args:
            value: The constant value (int, float, str, bool, None, etc.)
        """
        self.value = value

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with inferred dtype.

        Returns:
            {
                "type": "Literal",
                "value": self.value,
                "dtype": inferred_type_string
            }
        """
        return {"type": "Literal", "value": self.value, "dtype": self._infer_dtype()}

    def _infer_dtype(self) -> str:
        """
        Infer Arrow/DataFusion type from Python value.

        Returns:
            A type string: "Boolean", "Int64", "Float64", "String", "Null"
        """
        if isinstance(self.value, bool):
            # Must check bool before int (bool is subclass of int)
            return "Boolean"
        elif isinstance(self.value, int):
            return "Int64"
        elif isinstance(self.value, float):
            return "Float64"
        elif isinstance(self.value, str):
            return "String"
        elif self.value is None:
            return "Null"
        else:
            # Fallback: serialize as string
            return "String"


class BinOpExpr(Expr):
    """
    Represents a binary operation: +, -, >, <, ==, &, |, etc.

    Attributes:
        op (str): Operation name ("Add", "Gt", "And", etc.)
        left (Expr): Left operand
        right (Expr): Right operand
    """

    def __init__(self, op: str, left: Expr, right: Expr):
        """
        Initialize a BinOpExpr.

        Args:
            op: Operation name (e.g., "Add", "Gt", "And")
            left: Left operand (Expr)
            right: Right operand (Expr)
        """
        self.op = op
        self.left = left
        self.right = right

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of operands.

        Returns:
            {
                "type": "BinOp",
                "op": self.op,
                "left": self.left.serialize(),
                "right": self.right.serialize()
            }
        """
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

    def __init__(self, op: str, operand: Expr):
        """
        Initialize a UnaryOpExpr.

        Args:
            op: Operation name (e.g., "Not")
            operand: The operand (Expr)
        """
        self.op = op
        self.operand = operand

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of operand.

        Returns:
            {
                "type": "UnaryOp",
                "op": self.op,
                "operand": self.operand.serialize()
            }
        """
        return {"type": "UnaryOp", "op": self.op, "operand": self.operand.serialize()}


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
        """
        Initialize a CallExpr.

        Args:
            func: Function/method name (e.g., "shift", "rolling", "sum")
            args: Positional arguments (tuple, may contain Exprs)
            kwargs: Keyword arguments (dict, may contain Exprs)
            on: Object the method is called on, or None for functions
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.on = on

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of args/kwargs/on.

        Args are serialized if they're Exprs; literals are converted to LiteralExpr first.

        Returns:
            {
                "type": "Call",
                "func": self.func,
                "args": [...],  # serialized args
                "kwargs": {...},  # serialized kwargs
                "on": serialized_on or None
            }
        """

        def serialize_value(v):
            """Serialize a value: Expr -> dict, literal -> LiteralExpr -> dict"""
            if isinstance(v, Expr):
                return v.serialize()
            # Convert literal to LiteralExpr for serialization
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

        Args:
            method_name: Method to call on the result of this call

        Returns:
            A callable that accepts (*args, **kwargs) and returns CallExpr

        Raises:
            AttributeError: If method_name starts with underscore
        """
        if method_name.startswith("_"):
            raise AttributeError(f"No attribute {method_name}")

        def chained_call(*args, **kwargs):
            """Create a CallExpr with self as the 'on' target."""
            return CallExpr(method_name, args, kwargs, on=self)

        return chained_call

    def lookup(
        self, target_table: "Any", column: str, join_key: Optional[str] = None
    ) -> "LookupExpr":
        """
        Lookup a value in another table using the result of this call as the join key.

        Used to fetch a single column from a related table in derived expressions.

        Args:
            target_table: The target table object (LTSeq instance)
            column: Column name to fetch from the target table
            join_key: Column name in target table to join on (optional)

        Returns:
            A LookupExpr that can be used in derive() or other expressions

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> enriched = orders.derive(
            ...     product_name=lambda r: r.product_id.lower().lookup(products, "name")
            ... )
        """
        # If target_table is an LTSeq, we'll store just the table name for now
        # The actual resolution happens during derive
        target_name = getattr(target_table, "_name", "target")

        return LookupExpr(
            on=self,
            target_name=target_name,
            target_columns=[column],
            join_key=join_key,
        )


class LookupExpr(Expr):
    """
    Represents a lookup operation to fetch values from another table.

    Used as a shortcut for join operations in derived columns.
    Syntax: r.product_id.lookup(products, "name")

    This translates to an internal join where the left key is this expression
    and the right key is the primary key of the target table.

    Attributes:
        on (Expr): The expression to lookup (e.g., r.product_id)
        target_name (str): Name of the target table (for reference)
        target_columns (list): Columns to fetch from the target table
        join_key (str): Column name in target table to match against (default: primary key)
    """

    def __init__(
        self,
        on: Expr,
        target_name: str,
        target_columns: list,
        join_key: Optional[str] = None,
    ):
        """
        Initialize a LookupExpr.

        Args:
            on: The expression containing the lookup key(s)
            target_name: Name/reference to the target table
            target_columns: List of column names to fetch from target
            join_key: Column name in target to join on (optional, defaults to primary key)
        """
        self.on = on
        self.target_name = target_name
        self.target_columns = target_columns
        self.join_key = join_key

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict.

        Returns:
            {
                "type": "Lookup",
                "on": serialized_on,
                "target_name": target_name,
                "target_columns": target_columns,
                "join_key": join_key or null
            }
        """
        return {
            "type": "Lookup",
            "on": self.on.serialize(),
            "target_name": self.target_name,
            "target_columns": self.target_columns,
            "join_key": self.join_key,
        }
