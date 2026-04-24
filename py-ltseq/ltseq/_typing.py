from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .core import LTSeq


class LTSeqLike:
    # Shared attributes provided by LTSeq.
    _schema: dict[str, str]
    _inner: Any
    _sort_keys: list[tuple[str, bool]] | None
    _name: str | None

    def __len__(self) -> int: ...

    def _capture_expr(self, fn: Callable[..., Any]) -> dict[str, Any]: ...

    def filter(self, predicate: Callable[..., Any]) -> LTSeq: ...
    def sort(self, *args: Any, **kwargs: Any) -> LTSeq: ...
    def is_sorted_by(self, *keys: str, desc: bool | list[bool] = ...) -> bool: ...
    def to_arrow(self) -> Any: ...
