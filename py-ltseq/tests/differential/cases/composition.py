"""A literal next to an expression whose type comes from coercion.

Mostly mixed-type CASE expressions, alone and inside other expressions,
in the row, window and group dialects; also the declared dtype of a
derived column. ``_sw`` cases swap the CASE branches and negate the
condition, selecting the same values. Design review findings A, F1–F6
and F10 (#225).
"""

from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq, coalesce, if_else

CUT = datetime(1970, 1, 1, 0, 0, 1, 500000)


def ts():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0, 0],
        "s": pa.array([0, 1, 0, 0], pa.timestamp("s")),
        "s2": pa.array([0, 0, 0, 0], pa.timestamp("s")),
        "us": pa.array([1250000, 0, 1500000, None], pa.timestamp("us")),
    }))


def br(r):  # the seconds branch first
    return if_else(r.k > 0, r.s, r.us)


def br_sw(r):  # the same selection, microseconds first
    return if_else(r.k <= 0, r.us, r.s)


def case_case_type(): return ts().derive(v=br).select("v")
def case_eq(): return ts().derive(v=lambda r: br(r) == CUT).select("v")
def case_eq_sw(): return ts().derive(v=lambda r: br_sw(r) == CUT).select("v")
def case_lt(): return ts().derive(v=lambda r: br(r) < CUT).select("v")
def case_lt_sw(): return ts().derive(v=lambda r: br_sw(r) < CUT).select("v")
def case_literal_left(): return ts().derive(v=lambda r: CUT > br(r)).select("v")
def case_literal_left_sw(): return ts().derive(v=lambda r: CUT > br_sw(r)).select("v")
def case_inverted(): return ts().derive(v=lambda r: if_else(~(r.k > 0), r.us, r.s) == CUT).select("v")
def case_isin(): return ts().derive(v=lambda r: br(r).is_in([CUT])).select("v")
def case_isin_sw(): return ts().derive(v=lambda r: br_sw(r).is_in([CUT])).select("v")
def case_filter(): return ts().filter(lambda r: br(r) < CUT).select("k")
def case_filter_sw(): return ts().filter(lambda r: br_sw(r) < CUT).select("k")
def case_staged(): return ts().derive(c=br).derive(v=lambda r: r.c == CUT).select("v")
def case_staged_sw(): return ts().derive(c=br_sw).derive(v=lambda r: r.c == CUT).select("v")
def case_nested(): return ts().derive(v=lambda r: if_else(r.k > 1, r.s, if_else(r.k > 0, r.s, r.us)) == CUT).select("v")
def case_case_in_coalesce(): return ts().derive(v=lambda r: coalesce(br(r), r.s) == CUT).select("v")
def case_case_in_coalesce_sw(): return ts().derive(v=lambda r: coalesce(br_sw(r), r.s) == CUT).select("v")
def case_coalesce_in_case(): return ts().derive(v=lambda r: if_else(r.k > 0, coalesce(r.s, r.s), r.us) == CUT).select("v")
def case_case_in_fill_null(): return ts().derive(v=lambda r: br(r).fill_null(r.s) == CUT).select("v")


# The window dialect
def case_window_branch():
    return ts().sort("k").derive(v=lambda r: if_else(r.k > 0, r.s.shift(1), r.us) == CUT).select("v")


def case_window_branch_sw():
    return ts().sort("k").derive(v=lambda r: if_else(r.k <= 0, r.us, r.s.shift(1)) == CUT).select("v")


def case_shift_default():
    return ts().sort("k").derive(v=lambda r: br(r).shift(1, default=CUT)).select("v")


def case_shift_default_sw():
    return ts().sort("k").derive(v=lambda r: br_sw(r).shift(1, default=CUT)).select("v")


def lagged():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 2, 3],
        "t": pa.array([1, 1, 2, 2], pa.timestamp("s")),
        "u": pa.array([1000001, 1, 2000000, 2000001], pa.timestamp("us")),
    })).sort("k")


FINE_US = datetime(1970, 1, 1, 0, 0, 1, 1)


def case_lag_of_case(): return lagged().derive(v=lambda r: if_else(r.k > 0, r.t, r.u).shift(1) == FINE_US).select("v")
def case_lag_of_case_sw(): return lagged().derive(v=lambda r: if_else(r.k <= 0, r.u, r.t).shift(1) == FINE_US).select("v")


# dt.diff and subtraction with a CASE receiver
def case_dt_diff_column(): return ts().derive(v=lambda r: br(r).dt.diff(r.s2, unit="second")).select("v")
def case_dt_diff_column_sw(): return ts().derive(v=lambda r: br_sw(r).dt.diff(r.s2, unit="second")).select("v")
def case_dt_diff_literal(): return ts().derive(v=lambda r: br(r).dt.diff(datetime(1970, 1, 1), unit="second")).select("v")
def case_dt_diff_literal_sw(): return ts().derive(v=lambda r: br_sw(r).dt.diff(datetime(1970, 1, 1), unit="second")).select("v")
def case_subtract_literal(): return ts().derive(v=lambda r: br(r) - datetime(1970, 1, 1)).select("v")
def case_subtract_literal_sw(): return ts().derive(v=lambda r: br_sw(r) - datetime(1970, 1, 1)).select("v")


# Date and timestamp branches
def dates():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0],
        "d": pa.array([date(2024, 1, 1), date(2024, 1, 1), None], pa.date32()),
        "t": pa.array([datetime(2024, 1, 1, 6), datetime(2024, 1, 2), None], pa.timestamp("us")),
    }))


def case_date_timestamp_type(): return dates().derive(v=lambda r: if_else(r.k > 0, r.d, r.t)).select("v")
def case_date_timestamp_eq(): return dates().derive(v=lambda r: if_else(r.k > 0, r.d, r.t) == datetime(2024, 1, 1, 6)).select("v")
def case_date_timestamp_eq_sw(): return dates().derive(v=lambda r: if_else(r.k <= 0, r.t, r.d) == datetime(2024, 1, 1, 6)).select("v")


# Integer and float branches (the CASE executes as Float64)
def numbers_with_nan():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0, 0],
        "i": pa.array([None, 1, None, None], pa.int64()),
        "f": pa.array([float("nan"), None, 1e-16, None], pa.float64()),
    }))


def numbers():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 0],
        "i": pa.array([None, 1, None], pa.int64()),
        "f": pa.array([1e-16, None, 2.5], pa.float64()),
    }))


def ni(r): return if_else(r.k > 0, r.i, r.f)
def ni_sw(r): return if_else(r.k <= 0, r.f, r.i)


def case_number_type(): return numbers_with_nan().derive(v=ni).select("v")
def case_number_fill(): return numbers_with_nan().derive(v=lambda r: ni(r).fill_null(Decimal("2.5"))).select("v")
def case_number_fill_sw(): return numbers_with_nan().derive(v=lambda r: ni_sw(r).fill_null(Decimal("2.5"))).select("v")
def case_number_fill_staged(): return numbers_with_nan().derive(c=ni).derive(v=lambda r: r.c.fill_null(Decimal("2.5"))).select("v")
def case_number_gt_with_nan(): return numbers_with_nan().derive(v=lambda r: ni(r) > Decimal("0")).select("v")
def case_number_gt_with_nan_sw(): return numbers_with_nan().derive(v=lambda r: ni_sw(r) > Decimal("0")).select("v")
def case_number_gt(): return numbers().derive(v=lambda r: ni(r) > Decimal("0")).select("v")
def case_number_gt_sw(): return numbers().derive(v=lambda r: ni_sw(r) > Decimal("0")).select("v")
def case_number_eq(): return numbers().derive(v=lambda r: ni(r) == Decimal("2.5")).select("v")
def case_number_isin(): return numbers().derive(v=lambda r: ni(r).is_in([Decimal("2.5")])).select("v")
def case_number_isin_sw(): return numbers().derive(v=lambda r: ni_sw(r).is_in([Decimal("2.5")])).select("v")


# Narrow and wide decimal branches
def decimals():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 1],
        "p": pa.array([Decimal("1.23"), Decimal("0.12"), None], pa.decimal128(5, 2)),
        "pw": pa.array([Decimal("1000000000000000000000000000"), Decimal("0.1234567890"), None], pa.decimal128(38, 10)),
    }))


FINE20 = Decimal("0.12345678900000000000")


def case_decimal_eq(): return decimals().derive(v=lambda r: if_else(r.k > 0, r.p, r.pw) == FINE20).select("v")
def case_decimal_eq_sw(): return decimals().derive(v=lambda r: if_else(r.k <= 0, r.pw, r.p) == FINE20).select("v")
def case_decimal_eq_wide(): return decimals().derive(v=lambda r: if_else(r.k <= 0, r.p, r.pw) == FINE20).select("v")
def case_decimal_fill(): return decimals().derive(v=lambda r: if_else(r.k > 5, r.p, r.pw).fill_null(FINE20)).select("v")
def case_decimal_fill_sw(): return decimals().derive(v=lambda r: if_else(r.k <= 5, r.pw, r.p).fill_null(FINE20)).select("v")


# The group dialect: aggregates are typed exactly
def groups():
    return LTSeq.from_arrow(pa.table({
        "k": [0, 1, 2, 3],
        "g": [1, 1, 2, 2],
        "p": pa.array([Decimal("1.25"), Decimal("1.25"), Decimal("1.24"), Decimal("1.26")], pa.decimal128(5, 2)),
        "t": pa.array([1, 1, 2, 2], pa.timestamp("s")),
        "u": pa.array([1000001, 1, 2000000, 2000001], pa.timestamp("us")),
    })).sort("k")


def case_group_avg_decimal():
    return groups().group_ordered(lambda r: r.g).filter(lambda g: g.avg("p") == Decimal("1.25")).flatten().sort("k").select("k")


def case_group_max_seconds():
    return groups().group_ordered(lambda r: r.g).filter(lambda g: g.max("t") > FINE_US).flatten().sort("k").select("k")


def case_group_max_micros():
    return groups().group_ordered(lambda r: r.g).filter(lambda g: g.max("u") == FINE_US).flatten().sort("k").select("k")


# The dtype of a derived column does not depend on the row count
def case_empty_result_type(): return ts().derive(v=br).filter(lambda r: r.k > 100).select("v")
def case_empty_sorted_result_type(): return ts().derive(v=br).filter(lambda r: r.k > 100).sort("k").select("v")
def case_reverse_derived(): return ts().sort("k").derive(v=br).rvs().select("v")
def case_distinct_derived(): return ts().derive(v=br).distinct("v").sort("v").select("v")
def case_round_trip_derived(): return LTSeq.from_arrow(ts().derive(v=br).to_arrow()).select("v")
def case_slice_derived(): return ts().sort("k").derive(v=br).slice(0, 2).select("v")
