from typing import Any
from .base import Expr


class LiteralExpr(Expr):
    value: Any
    def __init__(self, value: Any) -> None: ...
    def serialize(self) -> dict[str, Any]: ...


class BinOpExpr(Expr):
    op: str
    left: Expr
    right: Expr
    def __init__(self, op: str, left: Expr, right: Expr) -> None: ...
    def serialize(self) -> dict[str, Any]: ...


class UnaryOpExpr(Expr):
    op: str
    operand: Expr
    def __init__(self, op: str, operand: Expr) -> None: ...
    def serialize(self) -> dict[str, Any]: ...
