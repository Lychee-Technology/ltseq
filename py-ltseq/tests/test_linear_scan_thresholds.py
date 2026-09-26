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
    """The direct kernel call must raise (so Python falls back), never return a truncated answer."""
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > threshold)
    with pytest.raises(RuntimeError, match="unsupported types Int64 and Float64"):
        steps._inner.group_ordered_count(expr)


def test_kernel_keeps_integral_float_thresholds(steps):
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > 2.0)
    assert steps._inner.group_ordered_count(expr) == 2


@pytest.mark.parametrize("threshold", ["1", Decimal("1")])
def test_kernel_keeps_int_parsing_string_and_decimal_thresholds(steps, threshold):
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > threshold)
    assert steps._inner.group_ordered_count(expr) == 3


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
    with pytest.raises(RuntimeError, match="unsupported types Int64 and Float64"):
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
