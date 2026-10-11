"""Shared fixtures: pinned engine layouts and input layouts (#274).

The engine's partition count defaults to the CPU count, so a result that
depends on it passes on a small machine and fails on a large one (#148).
These fixtures pin it, and the batch size, per test:

- ``engine_layout`` runs a test once per pinned layout: partition counts 1,
  2 and twice the CPU count the engine sees, crossed with batch sizes 1, 7
  and the engine default.
- ``engine_config`` pins one layout for the rest of a test:
  ``engine_config(target_partitions=2, batch_size=7)``.
- ``data_layout`` runs a test once per input layout of ``layouts.LAYOUTS``
  and builds the test's rows in it: ``t = data_layout(rows)``.

A table keeps the engine layout in effect when its reader or constructor
ran, so build tables in the test body, not in a fixture that runs before
the pin. The pin ends with the test.
"""

from __future__ import annotations

import contextlib
import itertools

import pytest

from ltseq._testing import pinned_engine

from . import layouts


@pytest.fixture(params=list(layouts.ENGINE_LAYOUTS.values()), ids=list(layouts.ENGINE_LAYOUTS))
def engine_layout(request):
    """Run the test at one pinned (partition count, batch size); yields the
    resulting ``ltseq._testing.engine_config()``."""
    target_partitions, batch_size = request.param
    with pinned_engine(target_partitions=target_partitions, batch_size=batch_size) as config:
        yield config


@pytest.fixture
def engine_config():
    """A function that pins the engine layout until the test ends."""
    with contextlib.ExitStack() as stack:

        def pin(*, target_partitions: int | None = None, batch_size: int | None = None):
            return stack.enter_context(
                pinned_engine(target_partitions=target_partitions, batch_size=batch_size)
            )

        yield pin


@pytest.fixture(params=layouts.LAYOUTS)
def data_layout(request, tmp_path):
    """A function that reads a ``pyarrow.Table`` back in this input layout;
    each call writes its files to a directory of its own."""
    directories = (tmp_path / f"input-{index}" for index in itertools.count())

    def build(rows):
        return layouts.build(request.param, rows, next(directories))

    return build
