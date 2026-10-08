"""Literals that DataFusion unifies with each other and with columns.

Design review finding B and F7 (#225): a Float64 literal next to a
decimal column, combined with a high-scale Decimal literal, and the
same unification with other numeric operands.
"""

from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq, coalesce, if_else

FINE33 = Decimal("1.230000000000000000000000000000000")


def x():
    return LTSeq.from_arrow(pa.table({"x": pa.array([None, Decimal("1.23")], pa.decimal128(5, 2))}))


def case_float_then_fine(): return x().derive(v=lambda r: coalesce(r.x, 1000000.0, FINE33)).select("v")
def case_fine_then_float(): return x().derive(v=lambda r: coalesce(r.x, FINE33, 1000000.0)).select("v")
def case_float_alone(): return x().derive(v=lambda r: coalesce(r.x, 1000000.0)).select("v")
def case_fine_alone(): return x().derive(v=lambda r: coalesce(r.x, FINE33)).select("v")
def case_small_float_then_fine(): return x().derive(v=lambda r: coalesce(r.x, 2.5, FINE33)).select("v")
def case_int_then_fine(): return x().derive(v=lambda r: coalesce(r.x, 10**18, FINE33)).select("v")


def case_two_decimals_too_wide():
    return x().derive(v=lambda r: coalesce(r.x, Decimal("123456789012345678901234"), Decimal("0.000000000000001"))).select("v")


def case_if_else_float(): return x().derive(v=lambda r: if_else(r.x.is_null(), 1000000.0, r.x)).select("v")
def case_isin_float_and_fine(): return x().derive(v=lambda r: r.x.is_in([1000000.0, FINE33])).select("v")
def case_isin_fine(): return x().derive(v=lambda r: r.x.is_in([FINE33])).select("v")


def columns():
    return LTSeq.from_arrow(pa.table({
        "x": pa.array([None, Decimal("1.23")], pa.decimal128(5, 2)),
        "f32": pa.array([None, None], pa.float32()),
        "u": pa.array([None, None], pa.uint64()),
    }))


def case_float32_column_and_fine(): return columns().derive(v=lambda r: coalesce(r.x, r.f32, FINE33)).select("v")
def case_uint64_column_and_fine(): return columns().derive(v=lambda r: coalesce(r.x, r.u, FINE33)).select("v")
def case_uint64_column_first_and_fine(): return columns().derive(v=lambda r: coalesce(r.u, r.x, FINE33)).select("v")
