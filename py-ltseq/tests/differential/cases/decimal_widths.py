"""Literals next to decimal columns of every Arrow width.

A literal is at most a Decimal128, but a column can be a Decimal32,
Decimal64 or Decimal256 (review F1 on #225: exact floats were refused for
those widths, and an integer fill rounded Decimal32/64 values to int64).
"""

from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq, coalesce, if_else


def column(dtype):
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1],
        "x": pa.array([Decimal("1.23"), None], dtype),
    })).sort("k")


D32 = pa.decimal32(9, 2)
D64 = pa.decimal64(18, 2)
D256 = pa.decimal256(20, 2)
D256W = pa.decimal256(76, 20)


# A shift() default the column holds exactly, and one it would round.
def case_shift_float_d32(): return column(D32).derive(v=lambda r: r.x.shift(1, default=1.5)).select("v")
def case_shift_float_d64(): return column(D64).derive(v=lambda r: r.x.shift(1, default=1.5)).select("v")
def case_shift_float_d256(): return column(D256).derive(v=lambda r: r.x.shift(1, default=1.5)).select("v")
def case_shift_float_d256w(): return column(D256W).derive(v=lambda r: r.x.shift(1, default=1.5)).select("v")
def case_shift_rounded_float_d32(): return column(D32).derive(v=lambda r: r.x.shift(1, default=1.236)).select("v")
def case_shift_rounded_float_d256(): return column(D256).derive(v=lambda r: r.x.shift(1, default=1.236)).select("v")
def case_shift_fine_float_d256w(): return column(D256W).derive(v=lambda r: r.x.shift(1, default=1.236)).select("v")


# Values that share a result column.
def case_fill_float_d256(): return column(D256).derive(v=lambda r: r.x.fill_null(1.5)).select("v")
def case_fill_float_d256w(): return column(D256W).derive(v=lambda r: r.x.fill_null(1.5)).select("v")
def case_fill_int_d32(): return column(D32).derive(v=lambda r: r.x.fill_null(1)).select("v")
def case_fill_int_d64(): return column(D64).derive(v=lambda r: r.x.fill_null(1)).select("v")
def case_fill_rounded_decimal_d32(): return column(D32).derive(v=lambda r: r.x.fill_null(Decimal("1.236"))).select("v")
# DataFusion has no common type for a Decimal32 and a scale-38 Decimal128.
def case_fill_tiny_decimal_d32(): return column(D32).derive(v=lambda r: r.x.fill_null(Decimal("1E-38"))).select("v")
def case_fill_exact_scale38_d32(): return column(D32).derive(v=lambda r: r.x.fill_null(Decimal("0.1" + "0" * 37))).select("v")


# Comparisons that DataFusion cannot type, casts the column for, or panics on.
def wide():
    return LTSeq.from_arrow(pa.table({
        "d32": pa.array([Decimal("-0.01"), Decimal("0.01"), None], pa.decimal32(9, 2)),
        "d64n": pa.array([Decimal(-100), Decimal(100), None], pa.decimal128(18, -2)).cast(pa.decimal64(18, -2)),
        "d256": pa.array([Decimal(1), Decimal(10) ** 75, None], pa.decimal256(76, 0)),
    }))


def case_tiny_decimal_d32(): return wide().derive(v=lambda r: r.d32 > Decimal("1E-38")).select("v")
def case_int_negative_scale_d64(): return wide().derive(v=lambda r: r.d64n == 7).select("v")
def case_decimal_d256(): return wide().derive(v=lambda r: r.d256 >= Decimal("1.2345")).select("v")
def case_float_d256(): return wide().derive(v=lambda r: r.d256 > 1.5).select("v")


# A coarse column next to a scale-38 literal needs 76 + 14 + 38 = 128
# digits, which overflows the i8 DataFusion computes a common precision in
# (review of 414926b on #225).
def coarse():
    return LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("1E14"), None], pa.decimal256(76, -14)),
    }))


def case_coarse_fill_zero(): return coarse().derive(v=lambda r: r.x.fill_null(Decimal("0E-38"))).select("v")
def case_coarse_coalesce_zero(): return coarse().derive(v=lambda r: coalesce(r.x, Decimal("0E-38"))).select("v")
def case_coarse_if_else_zero(): return coarse().derive(v=lambda r: if_else(r.x.is_null(), Decimal("0E-38"), r.x)).select("v")
def case_coarse_gt_tiny(): return coarse().derive(v=lambda r: r.x > Decimal("1E-38")).select("v")
def case_coarse_is_in_zero(): return coarse().derive(v=lambda r: r.x.is_in([Decimal("0E-38")])).select("v")
