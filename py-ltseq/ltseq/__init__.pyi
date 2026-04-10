# py-ltseq/ltseq/__init__.pyi
from typing import Any, Callable, overload

from .expr import (
    Expr,
    ColumnExpr,
    CallExpr,
    BinOpExpr,
    LiteralExpr,
    UnaryOpExpr,
    LookupExpr,
    SchemaProxy,
    NestedSchemaProxy,
    if_else,
    count_if,
    sum_if,
    avg_if,
    min_if,
    max_if,
    coalesce,
    nvl,
    ifa,
    row_number,
    rank,
    dense_rank,
    ntile,
    sqrt,
    power,
    sign,
    log,
    ln,
    exp,
    sin,
    cos,
    tan,
    asin,
    acos,
    atan,
    atan2,
    rand,
    gcd,
    lcm,
    factorial,
    str_char,
    concat_ws,
    now,
    today,
    skew,
    corr,
    covar,
    concat_agg,
)
from .cursor import Cursor
from .grouping.nested_table import NestedTable
from .linking import LinkedTable
from .partitioning import PartitionedTable, SQLPartitionedTable
from .aggregation import GroupBy


def seq(
    start_or_stop: int,
    stop: int | None = ...,
    step: int = ...,
) -> "LTSeq": ...


class LTSeq:
    # ------------------------------------------------------------------ schema
    @property
    def schema(self) -> dict[str, str]: ...
    @property
    def python_schema(self) -> dict[str, str]: ...
    @property
    def columns(self) -> list[str]: ...
    @property
    def dtypes(self) -> list[tuple[str, str]]: ...
    @property
    def sort_keys(self) -> list[tuple[str, bool]] | None: ...

    # ------------------------------------------------------------------ I/O
    @classmethod
    def read_csv(cls, path: str, has_header: bool = ...) -> "LTSeq": ...
    @classmethod
    def read_parquet(cls, path: str) -> "LTSeq": ...
    @classmethod
    def scan(cls, path: str, has_header: bool = ...) -> Cursor: ...
    @classmethod
    def scan_parquet(cls, path: str) -> Cursor: ...
    @classmethod
    def from_rows(
        cls,
        rows: list[dict[str, Any]],
        schema: dict[str, str] | None = ...,
    ) -> "LTSeq": ...
    @classmethod
    def from_dict(cls, data: dict[str, list[Any]]) -> "LTSeq": ...
    @classmethod
    def from_arrow(cls, arrow_table: Any) -> "LTSeq": ...
    @classmethod
    def from_pandas(cls, df: Any) -> "LTSeq": ...
    def write_csv(self, path: str) -> None: ...
    def write_parquet(self, path: str, compression: str | None = ...) -> None: ...
    def to_pandas(self) -> Any: ...
    def to_arrow(self) -> Any: ...
    def collect(self) -> list[dict[str, Any]]: ...

    # ------------------------------------------------------------------ display
    def show(self, n: int = ...) -> "LTSeq": ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    def count(self) -> int: ...

    # ------------------------------------------------------------------ transforms
    def filter(self, predicate: Callable[[SchemaProxy], Expr]) -> "LTSeq": ...
    def select(self, *cols: str | Callable[[SchemaProxy], list[Expr] | Expr]) -> "LTSeq": ...

    @overload
    def derive(self, __fn: Callable[[SchemaProxy], dict[str, Expr]]) -> "LTSeq": ...
    @overload
    def derive(self, **kwargs: Callable[[SchemaProxy], Expr]) -> "LTSeq": ...
    def derive(self, *args: Any, **kwargs: Any) -> "LTSeq": ...

    # Alias for derive
    @overload
    def with_columns(self, __fn: Callable[[SchemaProxy], dict[str, Expr]]) -> "LTSeq": ...
    @overload
    def with_columns(self, **kwargs: Callable[[SchemaProxy], Expr]) -> "LTSeq": ...
    def with_columns(self, *args: Any, **kwargs: Any) -> "LTSeq": ...

    def rename(
        self,
        mapping: dict[str, str] | None = ...,
        **kwargs: str,
    ) -> "LTSeq": ...
    def drop(self, *cols: str) -> "LTSeq": ...
    def sort(
        self,
        *keys: str | Callable[[SchemaProxy], Expr],
        desc: bool | list[bool] = ...,
        descending: bool | list[bool] | None = ...,
    ) -> "LTSeq": ...
    def assume_sorted(self, *keys: str, desc: bool | list[bool] = ...) -> "LTSeq": ...
    def distinct(self, *key_exprs: str | Callable[[SchemaProxy], Expr]) -> "LTSeq": ...
    def slice(self, offset: int = ..., length: int | None = ...) -> "LTSeq": ...
    def head(self, n: int = ...) -> "LTSeq": ...
    def tail(self, n: int = ...) -> "LTSeq": ...
    def is_sorted_by(self, *keys: str, desc: bool | list[bool] = ...) -> bool: ...
    def pipe(self, func: Callable[..., "LTSeq"], *args: Any, **kwargs: Any) -> "LTSeq": ...

    # ------------------------------------------------------------------ window / sequence ops
    def cum_sum(self, *cols: str | Callable[[SchemaProxy], Expr]) -> "LTSeq": ...
    def search_pattern(
        self,
        *step_predicates: Callable[[SchemaProxy], Expr],
        partition_by: str | None = ...,
    ) -> "LTSeq": ...
    def search_pattern_count(
        self,
        *step_predicates: Callable[[SchemaProxy], Expr],
        partition_by: str | None = ...,
    ) -> int: ...
    def search_first(self, predicate: Callable[[SchemaProxy], Expr]) -> "LTSeq | None": ...
    def stateful_scan(
        self,
        func: Callable[[Any, dict[str, Any]], Any],
        init: Any,
        output_col: str = ...,
    ) -> "LTSeq": ...

    # ------------------------------------------------------------------ joins
    def join(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy, SchemaProxy], Expr],
        how: str = ...,
        strategy: str | None = ...,
    ) -> "LTSeq": ...
    def semi_join(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy, SchemaProxy], Expr],
    ) -> "LTSeq": ...
    def anti_join(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy, SchemaProxy], Expr],
    ) -> "LTSeq": ...
    def asof_join(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy, SchemaProxy], Expr],
        direction: str = ...,
        is_sorted: bool = ...,
    ) -> "LTSeq": ...
    def link(
        self,
        target_table: "LTSeq",
        on: Callable[[SchemaProxy, SchemaProxy], Expr],
        as_: str,
        join_type: str = ...,
    ) -> LinkedTable: ...

    # ------------------------------------------------------------------ aggregation
    def agg(
        self,
        by: Callable[[SchemaProxy], Expr] | None = ...,
        **aggregations: Callable[[SchemaProxy], Expr],
    ) -> "LTSeq": ...
    def group_by(self, key: str | Callable[[SchemaProxy], Expr]) -> GroupBy: ...
    def group_ordered(self, grouping_fn: Callable[[SchemaProxy], Expr]) -> NestedTable: ...
    def group_consecutive(self, grouping_fn: Callable[[SchemaProxy], Expr]) -> NestedTable: ...
    def group_sorted(self, key: Callable[[SchemaProxy], Expr]) -> NestedTable: ...
    def partition(
        self,
        *args: str | Callable[[SchemaProxy], Expr],
        by: Callable[[SchemaProxy], Expr] | None = ...,
    ) -> PartitionedTable: ...
    def pivot(
        self,
        index: str | list[str],
        columns: str,
        values: str,
        agg_fn: str = ...,
    ) -> "LTSeq": ...

    # ------------------------------------------------------------------ set ops
    def union(self, other: "LTSeq") -> "LTSeq": ...
    def intersect(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy], Expr] | None = ...,
    ) -> "LTSeq": ...
    def except_(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy], Expr] | None = ...,
    ) -> "LTSeq": ...
    def xunion(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy], Expr] | None = ...,
    ) -> "LTSeq": ...
    def is_subset(
        self,
        other: "LTSeq",
        on: Callable[[SchemaProxy], Expr] | None = ...,
    ) -> bool: ...
    def contain(self, key_col: str, *values: Any) -> bool: ...
    def rvs(self) -> "LTSeq": ...
    def step(self, n: int) -> "LTSeq": ...
    def align(self, ref_sequence: list[Any], key: Callable[[SchemaProxy], Expr]) -> "LTSeq": ...

    # ------------------------------------------------------------------ mutation (copy-on-write)
    def insert(self, pos: int, row_dict: dict[str, Any]) -> "LTSeq": ...
    def delete(self, predicate_or_pos: Callable[[SchemaProxy], Expr] | int) -> "LTSeq": ...
    def update(
        self,
        predicate: Callable[[SchemaProxy], Expr],
        **updates: Any,
    ) -> "LTSeq": ...
    def modify(self, pos: int, **updates: Any) -> "LTSeq": ...


__all__ = [
    "LTSeq",
    "NestedTable",
    "LinkedTable",
    "PartitionedTable",
    "SQLPartitionedTable",
    "GroupBy",
    "Cursor",
    "SchemaProxy",
    "NestedSchemaProxy",
    "Expr",
    "ColumnExpr",
    "LiteralExpr",
    "BinOpExpr",
    "UnaryOpExpr",
    "CallExpr",
    "LookupExpr",
    "if_else",
    "count_if",
    "sum_if",
    "avg_if",
    "min_if",
    "max_if",
    "coalesce",
    "nvl",
    "ifa",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "sqrt",
    "power",
    "sign",
    "log",
    "ln",
    "exp",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "rand",
    "gcd",
    "lcm",
    "factorial",
    "str_char",
    "concat_ws",
    "now",
    "today",
    "skew",
    "corr",
    "covar",
    "concat_agg",
    "seq",
]
