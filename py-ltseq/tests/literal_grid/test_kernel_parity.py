"""The linear-scan kernel against the materialized group count (#189, #244).

``group_ordered(pred).first().count()`` runs the linear-scan kernel when the
predicate is eligible and otherwise materializes. The reference is the
materialized count. Every (table, column, literal, predicate) must agree,
except the cells of two open issues, which are pinned here so that a fix
or a new divergence both show.
"""

from __future__ import annotations

import operator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from . import grid

LITERALS = [
    "i0", "i1", "im1", "i2p53", "i2p53p1", "imax",
    "f0", "fm0", "f1", "f1_5", "f0_1", "f2p53", "fnan", "finf",
    "D1_5", "s1", "bT", "none", "dt_mid", "dt_utc",
]
COLUMNS = ["i32", "i64", "u32", "u64", "f32", "f64", "d5", "ts_us", "ts_ny", "str", "bool", "dict_i64", "date32"]
COMPARISONS = {
    "eq": operator.eq, "ne": operator.ne, "gt": operator.gt, "lt": operator.lt, "ge": operator.ge, "le": operator.le,
}
PREDICATES = list(COMPARISONS) + ["diff_gt", "ne_or_gt"]

# #189: the kernel's Gt arm on an exact integer column maps a NULL shift to
# false, where the materialized path starts a group at a NULL predicate.
# #244: the kernel's Float64 arms compare bit patterns, so -0.0 and 0.0
# differ in `eq`, `ne` and `gt`.
KNOWN = {}
for _col in ("i32", "i64", "u32"):
    for _lit in ("i0", "i1", "im1", "i2p53", "i2p53p1", "imax"):
        KNOWN[("nulls", _col, _lit, "gt")] = "#189"
for _lit, _pred in (("f0", "eq"), ("f0", "ne"), ("fm0", "eq"), ("fm0", "ne"), ("fm0", "gt")):
    KNOWN[("negzero", "f64", _lit, _pred)] = "#244"
KNOWN[("clean", "f64", "fm0", "diff_gt")] = "#244"


def predicates(col, value):
    c = lambda r: getattr(r, col)  # noqa: E731
    out = {name: (lambda r, op=op: op(c(r).shift(1), value)) for name, op in COMPARISONS.items()}
    out["diff_gt"] = lambda r: (c(r) - c(r).shift(1)) > value
    out["ne_or_gt"] = lambda r: (c(r) != c(r).shift(1)) | (c(r).shift(1) > value)
    return out


def tables():
    base = grid.arrow_table()
    yield "nulls", base
    filled = {}
    for name in base.column_names:
        column = base.column(name)
        if column.null_count:
            column = pc.fill_null(column, column.drop_null()[0])
        filled[name] = column
    yield "clean", pa.table(filled)
    yield "negzero", pa.table({
        "k": pa.array(range(6), pa.int64()),
        "f64": pa.array([1.0, 2.0, -0.0, 3.0, 0.0, 4.0], pa.float64()),
        "i64": pa.array([1, 2, 0, 3, 0, 4], pa.int64()),
    })


def count_outcome(fn):
    try:
        return ("ok", fn())
    except Exception as error:  # noqa: BLE001 - the class is the outcome
        return ("error", type(error).__name__)


@pytest.mark.parametrize("name,arrow", list(tables()), ids=lambda x: x if isinstance(x, str) else "")
def test_fast_count_matches_materialized_count(name, arrow):
    from ltseq import LTSeq

    t = LTSeq.from_arrow(arrow).sort("k")
    mismatches = {}
    for col in COLUMNS:
        if col not in arrow.column_names:
            continue
        for lit in LITERALS:
            value = grid.literals()[lit]
            for pred_name, pred in predicates(col, value).items():
                reference = count_outcome(lambda: t.group_ordered(pred).first().to_arrow().num_rows)
                fast = count_outcome(lambda: t.group_ordered(pred).first().count())
                if reference != fast:
                    mismatches[(name, col, lit, pred_name)] = (reference, fast)
    known = {cell for cell in KNOWN if cell[0] == name}
    unexpected = {cell: outcome for cell, outcome in mismatches.items() if cell not in known}
    assert not unexpected, "\n".join(f"{cell}: reference={ref} fast={fast}" for cell, (ref, fast) in unexpected.items())
    fixed = known - set(mismatches)
    assert not fixed, f"pinned mismatches that now agree (fixed?): {sorted(fixed)}"
