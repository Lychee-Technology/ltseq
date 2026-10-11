"""Fixtures and the summary of the memory harness.

These tests are marked ``memory`` and deselected by default (``pyproject.toml``)
because each run reads an input of ten budgets in a process of its own. Run
them with ``pytest py-ltseq/tests/memory -m memory``; CI runs them in a job of
their own.
"""

from __future__ import annotations

import pytest

from . import harness


@pytest.fixture(scope="session")
def harness_inputs(tmp_path_factory) -> tuple[harness.Source, harness.Source]:
    """The measured input (ten budgets, 8 files) and a warm-up input of the
    same schema and layout."""
    root = tmp_path_factory.mktemp("memory")
    files = 8
    source = harness.write_input(
        root / "input", harness.rows_for(harness.BUDGET, files=files), files=files
    )
    warmup = harness.write_input(root / "warmup", 4 * 1024, files=files)
    return source, warmup


@pytest.fixture
def measure(harness_inputs, tmp_path):
    """A function that measures one (pipeline, terminal) pair over the input."""
    source, warmup = harness_inputs

    def run(pipeline: str, terminal: str) -> harness.Measurement:
        return harness.measure(
            source, pipeline, terminal, warmup=warmup, out_dir=tmp_path / "out"
        )

    return run


def pytest_terminal_summary(terminalreporter):
    if harness.MEASUREMENTS:
        terminalreporter.section("peak memory above the level before the reader's call")
        for measurement in harness.MEASUREMENTS:
            terminalreporter.line(measurement.describe())
