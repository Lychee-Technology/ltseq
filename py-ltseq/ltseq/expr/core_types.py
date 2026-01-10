"""Core expression types: Literal, BinOp, UnaryOp."""

from typing import Any, Dict

from .base import Expr


class LiteralExpr(Expr):
    """
    Represents a constant value: int, float, str, bool, None.

    Attributes:
        value: The Python value
    """

    def __init__(self, value: Any):
        """Initialize a LiteralExpr with a constant value."""
        self.value = value

    def serialize(self) -> Dict[str, Any]:
        """Serialize to dict with inferred dtype."""
        return {"type": "Literal", "value": self.value, "dtype": self._infer_dtype()}

    def _infer_dtype(self) -> str:
        """Infer Arrow/DataFusion type from Python value."""
        if isinstance(self.value, bool):
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
        """Initialize a BinOpExpr with operation and operands."""
        self.op = op
        self.left = left
        self.right = right

    def serialize(self) -> Dict[str, Any]:
        """Serialize to dict with recursive serialization of operands."""
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
        """Initialize a UnaryOpExpr with operation and operand."""
        self.op = op
        self.operand = operand

    def serialize(self) -> Dict[str, Any]:
        """Serialize to dict with recursive serialization of operand."""
        return {"type": "UnaryOp", "op": self.op, "operand": self.operand.serialize()}
