"""Linear-scan fast path vs the DataFusion reference for float thresholds (issue #145 PR-4)."""

import math

import pyarrow as pa
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
    with pytest.raises(Exception):
        steps._inner.group_ordered_count(expr)


def test_kernel_keeps_integral_float_thresholds(steps):
    expr = steps._capture_expr(lambda r: (r.x - r.x.shift(1)) > 2.0)
    assert steps._inner.group_ordered_count(expr) == 2
