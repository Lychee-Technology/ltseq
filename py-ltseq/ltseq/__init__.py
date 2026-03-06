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
    # Math functions
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
    # Number-theory functions
    gcd,
    lcm,
    factorial,
    # String functions
    str_char,
    concat_ws,
    # Datetime functions
    now,
    today,
    # Null / conditional utilities
    nvl,
    ifa,
    # Aggregate expression functions
    skew,
    corr,
    covar,
    concat_agg,
)

# GPU utilities
try:
    from .ltseq_core import gpu_available
except ImportError:
    def gpu_available() -> bool:
        return False

# GPU table — conditional import (requires cudf)
try:
    from .gpu_table import GpuTable
    _GPUTABLE_AVAILABLE = True
except ImportError:
    _GPUTABLE_AVAILABLE = False

# Re-export utility functions
from .utils import seq

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
    "GpuTable",
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
    "nvl",
    "ifa",
    "skew",
    "corr",
    "covar",
    "concat_agg",
    "seq",
]
