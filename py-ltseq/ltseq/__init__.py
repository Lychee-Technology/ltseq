"""LTSeq: Time-series friendly data transformation library."""

# Re-export core classes for backward compatibility
from .core import LTSeq
from .grouping import NestedTable
from .linking import LinkedTable
from .partitioning import PartitionedTable

# GPU availability check (returns False if compiled without gpu feature)
from . import ltseq_core as _core

gpu_available = _core.gpu_available
gpu_list = _core.gpu_list
gpu_info = _core.gpu_info

# Re-export public expression types
from .expr import (
    SchemaProxy,
    BinOpExpr,
    ColumnExpr,
    UnaryOpExpr,
    CallExpr,
    Expr,
    if_else,
    count_if,
    sum_if,
    avg_if,
    min_if,
    max_if,
    coalesce,
    row_number,
    rank,
    dense_rank,
    ntile,
)

__all__ = [
    "LTSeq",
    "NestedTable",
    "LinkedTable",
    "PartitionedTable",
    "gpu_available",
    "gpu_list",
    "gpu_info",
    "SchemaProxy",
    "BinOpExpr",
    "ColumnExpr",
    "UnaryOpExpr",
    "CallExpr",
    "Expr",
    "if_else",
    "count_if",
    "sum_if",
    "avg_if",
    "min_if",
    "max_if",
    "coalesce",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
]
