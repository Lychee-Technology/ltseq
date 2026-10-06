"""Constant folding against the same arithmetic on a column (#193, #209),
and the linear-scan count against the materialized groups.
"""

import pyarrow as pa

from ltseq import LTSeq
from ltseq.expr import LiteralExpr


def ints():
    return LTSeq.from_arrow(pa.table({"x": pa.array([2**53 + 1, 1], pa.int64())}))


def case_fold_int(): return ints().derive(v=lambda r: LiteralExpr(2**53) + 1).select("v")
def case_fold_int_compare(): return ints().derive(v=lambda r: r.x == LiteralExpr(2**53) + 1).select("v")
def case_fold_div(): return ints().derive(v=lambda r: LiteralExpr(7) / 2).select("v")
def case_column_div(): return ints().derive(v=lambda r: (r.x * 0 + 7) / 2).select("v")
def case_fold_floordiv(): return ints().derive(v=lambda r: LiteralExpr(-7) // 2).select("v")
def case_column_floordiv(): return ints().derive(v=lambda r: (r.x * 0 - 7) // 2).select("v")
def case_fold_mod(): return ints().derive(v=lambda r: LiteralExpr(-7) % 2).select("v")
def case_column_mod(): return ints().derive(v=lambda r: (r.x * 0 - 7) % 2).select("v")
def case_fold_overflow(): return ints().derive(v=lambda r: LiteralExpr(2**62) * 4).select("v")
def case_column_overflow(): return ints().derive(v=lambda r: (r.x * 0 + 2**62) * 4).select("v")
def case_fold_float(): return ints().derive(v=lambda r: LiteralExpr(2.0) + 3.0).select("v")
def case_boolean_identity(): return ints().derive(v=lambda r: (r.x > 1) & True).select("v")
def case_boolean_identity_on_integer(): return ints().derive(v=lambda r: r.x & True).select("v")


def scan_table():
    return LTSeq.from_arrow(pa.table({"k": list(range(6)), "x": [1, 2, 4, 3, 2**53 + 1, 2**53]})).sort("k")


def both_counts(pred):
    t = scan_table()
    first = t.group_ordered(pred).first()
    return {"count": first.count(), "rows": first.to_arrow().num_rows}


def case_scan_int_div(): return both_counts(lambda r: r.x / 2 > r.x.shift(1) / 2)
def case_scan_float_add(): return both_counts(lambda r: r.x > r.x.shift(1) + 0.5)
def case_scan_big(): return both_counts(lambda r: r.x != r.x.shift(1))
def case_scan_mod(): return both_counts(lambda r: (r.x % 2) > (r.x.shift(1) % 2))
def case_scan_floordiv(): return both_counts(lambda r: (r.x // 2) > (r.x.shift(1) // 2))
def case_scan_float_multiply(): return both_counts(lambda r: r.x > r.x.shift(1) * 1.0)
