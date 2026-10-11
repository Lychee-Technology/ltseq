"""The memory harness, end to end (#274).

Row R3 of the v0.5 contract asks that every MUST-stream pipeline of §21.1,
drained by every draining terminal, stays within a fixed memory budget.
Implementing R3 belongs to M1 and the items that bring v0.5's terminals.
Until then these tests keep the harness honest: one MUST-stream pipeline
runs end to end and must stay within the budget, and a pipeline that
materializes its input must exceed it. The §21.1 SHOULD bounds are measured
and reported, never failed, since a SHOULD is not a gate.
"""

from __future__ import annotations

import warnings

import pytest

from . import catalog, harness

pytestmark = [
    pytest.mark.memory,
    pytest.mark.skipif(not harness.SUPPORTED, reason="the peak reset needs Linux"),
]


@pytest.mark.parametrize(
    "terminal",
    [
        "to_batches",
        "count",
        "write_parquet",
        pytest.param(
            "write_csv",
            marks=pytest.mark.xfail(
                raises=harness.BudgetExceeded,
                strict=True,
                reason="write_csv collects the whole result before writing; M1 makes it stream (#148)",
            ),
        ),
        pytest.param(
            "iterate",
            marks=pytest.mark.xfail(
                raises=harness.BudgetExceeded,
                strict=True,
                reason="iteration converts the whole result with to_dicts; §15.2 streams it through to_batches (M1)",
            ),
        ),
    ],
)
def test_must_stream_pipeline_stays_within_the_budget(measure, harness_inputs, terminal):
    source, _ = harness_inputs
    measurement = measure("filter_select_derive", terminal)
    harness.assert_within_budget(measurement)
    assert measurement.drained == source.rows * 3 // 4


def test_a_materializing_pipeline_exceeds_the_budget(measure):
    measurement = measure("collect", "to_batches")
    with pytest.raises(harness.BudgetExceeded):
        harness.assert_within_budget(measurement)


@pytest.mark.parametrize("pipeline", [*catalog.SHOULD, *catalog.U2])
def test_should_bound_is_reported(measure, pipeline):
    measurement = measure(pipeline, "to_batches")
    if not measurement.within_budget:
        warnings.warn(harness.ShouldBoundExceeded(measurement.describe()))
