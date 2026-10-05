"""`== None` / `!= None` type as the null checks they build (#154).

At runtime `expr == None` returns `expr.is_null()`, a CallExpr, not the
BinOpExpr every other comparison returns. The stub overloads must say so.
"""

from typing import assert_type

from ltseq.expr.core_types import BinOpExpr
from ltseq.expr.types import CallExpr, ColumnExpr


def null_comparisons(c: ColumnExpr) -> None:
    assert_type(c == None, CallExpr)  # noqa: E711
    assert_type(c != None, CallExpr)  # noqa: E711
    assert_type(c == 1, BinOpExpr)
    assert_type(c != c, BinOpExpr)
    assert_type((c == None) | (c > 1), BinOpExpr)  # noqa: E711
