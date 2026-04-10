from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .core import LTSeq


class GroupBy:
    def __init__(self, table: LTSeq, key: str | Callable) -> None: ...
    def agg(self, **aggregations: Callable) -> LTSeq: ...
