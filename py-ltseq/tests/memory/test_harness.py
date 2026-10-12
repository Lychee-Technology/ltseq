"""The memory harness's checks on its inputs (#274).

Nothing here starts a child process, so these tests are not marked
``memory``: they run in the default suite, on every platform.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from . import catalog, harness
from .harness import BUDGET, MiB


def source(nbytes: int) -> harness.Source:
    return harness.Source("parquet", Path("unused"), nbytes // harness.ROW_BYTES, nbytes)


def baseline(peak: int) -> harness.Measurement:
    return harness.Measurement(
        "assume_sorted",
        "to_batches",
        harness.R3_BUDGET,
        peak_above=peak,
        aborted=False,
        drained=None,
        seconds=None,
    )


# The state bounds start from a baseline the conftest holds to the budget.
# 49 MiB is within it and puts a bound above a tenth of R3's input.
@pytest.mark.parametrize("peak", [0, 49 * MiB, BUDGET], ids=["0", "49MiB", "budget"])
def test_an_input_r3_accepts_fits_every_state_bound(peak):
    for state_rows in catalog.STATE_ROWS.values():
        bound = harness.state_bound(baseline(peak), state_rows)
        assert bound.min_input <= harness.R3_BUDGET.min_input, bound


BOUND = harness.state_bound(baseline(49 * MiB), 1024)


# R3's budget refuses an input below ten budgets. A state bound refuses one
# no larger than itself, which a run could hold without going over.
@pytest.mark.parametrize(
    ("limit", "nbytes"),
    [(harness.R3_BUDGET, 10 * BUDGET - 1), (BOUND, BOUND.nbytes)],
    ids=["r3_budget", "state_bound"],
)
def test_measure_refuses_an_input_too_small_for_its_limit(limit, nbytes, tmp_path):
    with pytest.raises(ValueError, match="needs an input"):
        harness.measure(
            source(nbytes),
            "filter_select_derive",
            "to_batches",
            warmup=source(0),
            out_dir=tmp_path / "out",
            limit=limit,
        )
