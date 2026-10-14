"""The counting kernel's float comparisons against the DataFusion path (#244).

`group_ordered(pred).first().count()` counts with the linear-scan kernel
when `can_linear_scan` admits the predicate, and otherwise materializes the
grouped table, whose count is the reference. DataFusion compares floats by
IEEE 754 totalOrder except that `-0.0` equals `0.0`. The kernel used to call
Arrow's comparison kernels directly, which apply totalOrder without that
exception and so put `-0.0` below `0.0`: a zero that changed sign started a
group for the kernel only. `count()` falls back only when the kernel raises,
so its count reached the user.

The predicates cover every Float64 shape the kernel admits (`==`, `!=` and
`>` between a column, its `shift(1)`, a literal and their difference) over
data where the two zeros meet each other, NULL, NaN and infinity, in both
orders.

The NaNs differ in sign and in payload, in the data and in the literals.
Both paths compare NaNs bit for bit today, while the v0.5 contract (§18)
makes every NaN one value (#290). The kernel follows the reference only as
long as that change happens inside `apply_cmp`; if it happens anywhere
else, these cases fail until the kernel changes too.
"""

from __future__ import annotations

import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

DECLINED = "only supports shift-based boundary predicates"

NAN = math.nan
NEG_NAN = math.copysign(math.nan, -1.0)
# A quiet NaN whose payload is 1 rather than 0.
OTHER_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF8_0000_0000_0001))[0]
INF = math.inf

X = [
    1.0, -0.0, 0.0, 0.0, -0.0, -0.0, None, 0.0, 2.0,
    NAN, NAN, NEG_NAN, NAN, OTHER_NAN, NAN, INF, -0.0, 1.0, 0.0,
]
Y = [
    0.0, 0.0, -0.0, 1.0, 0.0, None, -0.0, -0.0, 2.0,
    0.0, NAN, 1.0, OTHER_NAN, NAN, NEG_NAN, -0.0, 0.0, 3.0, -0.0,
]

COMPARISONS = {
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
}
LITERALS = {
    "0.0": 0.0,
    "-0.0": -0.0,
    "nan": NAN,
    "-nan": NEG_NAN,
    "nan with payload 1": OTHER_NAN,
    "inf": INF,
    "1.0": 1.0,
}


def predicates() -> dict:
    out = {}
    for sym, cmp in COMPARISONS.items():
        out[f"x {sym} x.shift(1)"] = lambda r, cmp=cmp: cmp(r.x, r.x.shift(1))
        out[f"x.shift(1) {sym} x"] = lambda r, cmp=cmp: cmp(r.x.shift(1), r.x)
        out[f"y {sym} x.shift(1)"] = lambda r, cmp=cmp: cmp(r.y, r.x.shift(1))
        for name, value in LITERALS.items():
            out[f"x.shift(1) {sym} {name}"] = lambda r, cmp=cmp, v=value: cmp(r.x.shift(1), v)
        for name in ("0.0", "-0.0"):
            value = LITERALS[name]
            out[f"(x - x.shift(1)) {sym} {name}"] = (
                lambda r, cmp=cmp, v=value: cmp(r.x - r.x.shift(1), v)
            )
            out[f"(x.shift(1) - y) {sym} {name}"] = (
                lambda r, cmp=cmp, v=value: cmp(r.x.shift(1) - r.y, v)
            )
    out["(x != x.shift(1)) & (y == y.shift(1))"] = lambda r: (r.x != r.x.shift(1)) & (r.y == r.y.shift(1))
    out["~(x == x.shift(1)) | (y > -0.0)"] = lambda r: ~(r.x == r.x.shift(1)) | (r.y > -0.0)
    return out


PREDICATES = predicates()


def rows(x=X, y=Y) -> pa.Table:
    return pa.table({
        "i": pa.array(range(len(x)), pa.int64()),
        "x": pa.array(x, pa.float64()),
        "y": pa.array(y, pa.float64()),
    })


def reference(t: LTSeq, pred) -> int:
    return t.group_ordered(pred).first().to_arrow().num_rows


def kernel(t: LTSeq, pred) -> int:
    """The kernel's count. Every predicate here is one the kernel admits, so
    a decline fails the test instead of passing it vacuously."""
    try:
        return t._inner.group_ordered_count(t._capture_expr(pred))
    except ValueError as error:
        if DECLINED in str(error):
            pytest.fail(f"the kernel declined a Float64 predicate: {error}")
        raise


def assert_kernel_matches_reference(t: LTSeq):
    mismatches = []
    for name, pred in PREDICATES.items():
        fast, expected = kernel(t, pred), reference(t, pred)
        if fast != expected:
            mismatches.append(f"{name}: kernel={fast} reference={expected}")
    assert not mismatches, "\n".join(mismatches)


def test_kernel_compares_floats_as_the_reference_does():
    assert_kernel_matches_reference(LTSeq.from_arrow(rows()).sort("i"))


@pytest.mark.parametrize("row_group_size", [1, 3, 1000])
def test_kernel_compares_floats_as_the_reference_does_on_sorted_parquet(tmp_path, row_group_size):
    # Float predicates have no fused form: the parallel Parquet count hands
    # them to the general linear-scan path.
    path = str(tmp_path / "floats.parquet")
    pq.write_table(rows(), path, row_group_size=row_group_size)
    assert_kernel_matches_reference(LTSeq.read_parquet(path).assume_sorted("i"))


# The repro in #244 and the shapes its parity cells pinned, with the
# reference's counts. Before the fix the kernel counted 2, 1, 5 and 3 for the
# four cases with a `-0.0`; the two without one are controls.
REGRESSION_ROWS = {
    "-0.0": [1.0, 2.0, -0.0, 3.0, 0.0, 4.0],
    "0.0": [1.0, 2.0, 0.0, 3.0, 0.0, 4.0],
    "NULL": [1.0, None, 0.0, 3.0, 0.0, 4.0],
    "adjacent zeros": [0.0, -0.0, 0.0, -0.0, 1.0],
}
REGRESSIONS = {
    # `x.shift(1) == 0.0` is true after either zero.
    "-0.0 data, 0.0 literal": ("-0.0", lambda r: r.x.shift(1) == 0.0, 3),
    "0.0 data, 0.0 literal": ("0.0", lambda r: r.x.shift(1) == 0.0, 3),
    "NULL data, 0.0 literal": ("NULL", lambda r: r.x.shift(1) == 0.0, 4),
    "0.0 data, -0.0 literal": ("0.0", lambda r: r.x.shift(1) == -0.0, 3),
    # A zero that changes sign is not a change, and `0.0 > -0.0` is false.
    "changes": ("adjacent zeros", lambda r: r.x != r.x.shift(1), 2),
    "gap over -0.0": ("adjacent zeros", lambda r: (r.x - r.x.shift(1)) > -0.0, 2),
}


@pytest.mark.parametrize("name", REGRESSIONS)
def test_regressions_count_signed_zeros_as_one_value(data_layout, name):
    data, pred, expected = REGRESSIONS[name]
    x = REGRESSION_ROWS[data]
    t = data_layout(rows(x, [0.0] * len(x))).sort("i")
    assert reference(t, pred) == expected
    assert kernel(t, pred) == expected
    assert t.group_ordered(pred).first().count() == expected
