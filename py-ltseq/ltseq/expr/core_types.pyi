from typing import Any
from .base import Expr


class LiteralExpr(Expr):
    value: Any
    def __init__(self, value: Any) -> None: ...
    def serialize(self) -> dict[str, Any]: ...


class BinOpExpr(Expr):
    op: str
    left: Any
    right: Any
    def __init__(self, op: str, left: Any, right: Any) -> None: ...
    def serialize(self) -> dict[str, Any]: ...


class UnaryOpExpr(Expr):
    op: str
    operand: Any
    def __init__(self, op: str, operand: Any) -> None: ...
    def serialize(self) -> dict[str, Any]: ...
