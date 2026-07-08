from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    from .core import LTSeq

# Enum-like string parameters, shared by runtime annotations and the .pyi stubs.
JoinHow = Literal["inner", "left", "right", "full"]


class LTSeqLike:
    # Shared members provided by LTSeq. _schema and _sort_keys are lazy
    # properties backed by the Rust kernel (issue #93).
    _inner: Any
    _name: str | None

    @property
    def _schema(self) -> dict[str, str]: ...

    @property
    def _sort_keys(self) -> list[tuple[str, bool]] | None: ...

    def __len__(self) -> int: ...

    def _capture_expr(self, fn: Callable[..., Any]) -> dict[str, Any]: ...

    def to_dicts(self) -> list[dict[str, Any]]: ...

    def filter(self, predicate: Callable[..., Any]) -> LTSeq: ...
    def sort(self, *args: Any, **kwargs: Any) -> LTSeq: ...
    def is_sorted_by(self, *keys: str, desc: bool | list[bool] = ...) -> bool: ...
    def to_arrow(self) -> Any: ...
