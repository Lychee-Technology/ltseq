"""
Expression system for LTSeq: Captures Python lambdas as serializable AST trees.

This module provides a set of classes that intercept Python operators and method
calls without executing them. Instead, it builds a tree representation that Phase 4
will transpile to DataFusion logical plans.

Core Classes:
  - Expr: Abstract base class for all expressions
  - ColumnExpr: Column reference (e.g., r.age)
  - LiteralExpr: Constant value (e.g., 42, "hello")
  - BinOpExpr: Binary operation (e.g., r.age + 5, r.price > 10)
  - UnaryOpExpr: Unary operation (e.g., ~r.flag)
  - CallExpr: Function/method call (e.g., r.col.shift(1), r.col.rolling(3).mean())
  - SchemaProxy: Row proxy for capturing expressions in lambdas

Example:
  >>> schema = {"age": "int64", "name": "string"}
  >>> r = SchemaProxy(schema)
  >>> expr = r.age > 18
  >>> expr.serialize()
  {'type': 'BinOp', 'op': 'Gt', 'left': {'type': 'Column', 'name': 'age'}, ...}
"""

# Re-export base expression class
from .base import Expr

# Re-export concrete expression types
from .types import BinOpExpr, CallExpr, ColumnExpr, LiteralExpr, UnaryOpExpr

# Re-export schema proxies
from .proxy import NestedSchemaProxy, SchemaProxy

# Re-export transformation and conversion functions
from .transforms import _lambda_to_expr, _transform_lambda_for_none_checks

__all__ = [
    "Expr",
    "ColumnExpr",
    "LiteralExpr",
    "BinOpExpr",
    "UnaryOpExpr",
    "CallExpr",
    "SchemaProxy",
    "NestedSchemaProxy",
    "_lambda_to_expr",
    "_transform_lambda_for_none_checks",
]
