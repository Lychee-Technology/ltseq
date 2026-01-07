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
