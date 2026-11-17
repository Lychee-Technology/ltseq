
from .types import ColumnExpr, CallExpr


class _SchemaAttr(ColumnExpr):
    def __getattr__(self, name: str) -> ColumnExpr: ...


class SchemaProxy:
    def __init__(self, schema: dict[str, str]) -> None: ...
    def __getattr__(self, name: str) -> _SchemaAttr: ...
    def get_schema(self) -> dict[str, str]: ...


class NestedSchemaProxy:
    def __init__(self, alias: str, prefixed_columns: dict[str, str]) -> None: ...
    def __getattr__(self, name: str) -> ColumnExpr: ...


class _GroupCount(_SchemaAttr):
    """``g.count``: ``g.count()`` counts the group's rows; ``g.count.sum()``
    (a column named ``count``) and ``g.count.v`` (a ``count_v`` column of a
    link with ``as_="count"``) read columns."""
    def __call__(self) -> CallExpr: ...


class GroupAggProxy(SchemaProxy):
    """The ``g`` of ``group_by().agg()`` and ``LTSeq.agg()`` lambdas."""
    count: _GroupCount
