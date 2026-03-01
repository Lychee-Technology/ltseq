"""LTSeq: Time-series friendly data transformation library."""

# Re-export core classes for backward compatibility
from .core import LTSeq
from .grouping import NestedTable
from .linking import LinkedTable
from .partitioning import PartitionedTable

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

# GPU utilities
from .ltseq_core import gpu_available

__all__ = [
    "LTSeq",
    "NestedTable",
    "LinkedTable",
    "PartitionedTable",
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
    "gpu_available",
]
