from typing import Any
from .base import Expr


class LookupExpr(Expr):
    def __init__(
        self,
        on: Expr,
        target_table: Any,
        target_name: str,
        target_columns: list[str],
        join_key: str | None = ...,
    ) -> None: ...
    def serialize(self) -> dict[str, Any]: ...
    @staticmethod
    def clear_registry() -> None: ...
