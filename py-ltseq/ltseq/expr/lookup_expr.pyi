from typing import Any
from .base import Expr


class LookupExpr(Expr):
    on: Expr
    target_table: Any
    target_name: str
    target_columns: list[str]
    join_key: str | None

    def __init__(
        self,
        on: Expr,
        target_table: Any,
        target_name: str,
        target_columns: list[str],
        join_key: str | None = ...,
    ) -> None: ...
    def serialize(self) -> dict[str, Any]: ...
    @classmethod
    def get_table(cls, name: str) -> Any: ...
    @classmethod
    def clear_registry(cls) -> None: ...
