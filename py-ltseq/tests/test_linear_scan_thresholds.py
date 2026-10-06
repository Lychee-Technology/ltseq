"""Linear-scan fast path vs the DataFusion reference for float thresholds (issue #145 PR-4)."""

import math
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq


@pytest.fixture
def steps():
    return LTSeq.from_arrow(pa.table({"x": pa.array([1, 1, 3, 3, 4, 10, 10], pa.int64())})).sort("x")


def _reference(t, pred):
    return len(t.group_ordered(pred).first().to_pandas())


@pytest.mark.parametrize(
    "threshold, expected",
    # x = [1, 1, 3, 3, 4, 10, 10]: diffs are [null, 0, 2, 0, 1, 6, 0]; row 0 is always a boundary.
    [(-0.5, 7), (1.5, 3), (2.0, 2), (2, 2), (math.nan, 1), (math.inf, 1), (-math.inf, 7)],
)
def test_first_count_matches_reference_for_float_thresholds(steps, threshold, expected):
    pred = lambda r: (r.x - r.x.shift(1)) > threshold  # noqa: E731
    assert _reference(steps, pred) == expected
    assert steps.group_ordered(pred).first().count() == expected


@pytest.mark.parametrize("threshold", [-0.5, 1.5, math.nan, math.inf, -math.inf])
def test_kernel_declines_non_integral_thresholds(steps, threshold):
    """The direct kernel call must raise (so Python falls back), never return a
    truncated answer. It declines before scanning: an integer compared with a
    float the kernel cannot read exactly is not eligible."""
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > threshold)
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        steps._inner.group_ordered_count(expr)


def test_kernel_keeps_integral_float_thresholds(steps):
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > 2.0)
    assert steps._inner.group_ordered_count(expr) == 2


@pytest.mark.parametrize(
    "threshold, expected", [(Decimal("1"), 3), (Decimal("1.00"), 3), (Decimal("1.5"), 3), (Decimal("0.5"), 4)]
)
def test_decimal_thresholds_take_the_datafusion_path(steps, threshold, expected):
    """The kernel has no decimal arithmetic, so it declines every Decimal
    literal up front, integral ones included, and the count falls back."""
    pred = lambda r: (r.x - r.x.shift(1)) > threshold  # noqa: E731
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        steps._inner.group_ordered_count(steps._capture_expr(pred))
    assert steps.group_ordered(pred).first().count() == _reference(steps, pred) == expected


@pytest.mark.parametrize("divisor", [Decimal("2"), Decimal("2.0")])
def test_decimal_arithmetic_count_matches_reference(divisor):
    """`x / Decimal("2")` keeps the fraction: as an integer the kernel would
    compare the quotients [1, 1, 2, 2] and count 2 groups instead of 4."""
    t = LTSeq.from_arrow(pa.table({"k": range(4), "x": [2, 3, 4, 5]})).sort("k")
    pred = lambda r: (r.x / divisor) > (r.x.shift(1) / divisor)  # noqa: E731
    assert t.derive(v=pred).to_arrow().column("v").to_pylist() == [None, True, True, True]
    assert t.group_ordered(pred).first().count() == _reference(t, pred) == 4


def test_string_threshold_is_not_a_number_for_the_kernel(steps):
    """A string literal is a string (#145, P5): the kernel declines it, and the
    DataFusion path, which coerces the string, counts the groups."""
    pred = lambda r: (r.x - r.x.shift(1)) > "1"  # noqa: E731
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        steps._inner.group_ordered_count(steps._capture_expr(pred))
    assert steps.group_ordered(pred).first().count() == _reference(steps, pred) == 3


# Beyond 2**53 the Float64 reference rounds the Int64 diff before comparing, so an
# i64 comparison on the fast path would count a boundary the reference does not.
@pytest.mark.parametrize(
    "xs, threshold",
    [([0, 2**53 + 1], 2.0**53), ([0, 10**18 + 1], 1e18), ([2**53 + 1, 0], -(2.0**53))],
)
def test_first_count_matches_reference_beyond_float_precision(xs, threshold):
    t = LTSeq.from_arrow(pa.table({"i": pa.array(range(len(xs)), pa.int64()), "x": pa.array(xs, pa.int64())})).sort("i")
    pred = lambda r: (r.x - r.x.shift(1)) > threshold  # noqa: E731
    expected = _reference(t, pred)
    assert expected == 1
    assert t.group_ordered(pred).first().count() == expected
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        t._inner.group_ordered_count(t._capture_expr(pred))


def test_kernel_keeps_integral_float_thresholds_below_2_pow_53():
    t = LTSeq.from_arrow(pa.table({"x": pa.array([0, 2**53 - 1], pa.int64())})).sort("x")
    expr = t._capture_expr(lambda r: (r.x - r.x.shift(1)) > 2.0**53 - 2)
    assert t._inner.group_ordered_count(expr) == 2


def test_parquet_scan_matches_reference_beyond_float_precision(tmp_path):
    path = str(tmp_path / "big.parquet")
    pq.write_table(pa.table({"x": pa.array([0, 2**53 + 1], pa.int64())}), path)
    t = LTSeq.read_parquet(path).assume_sorted("x")
    pred = lambda r: (r.x - r.x.shift(1)) > 2.0**53  # noqa: E731
    assert t.group_ordered(pred).first().count() == _reference(t, pred) == 1
