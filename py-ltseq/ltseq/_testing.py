"""Test-only engine configuration (#274). Not public API.

The engine's parallelism (DataFusion ``target_partitions``, by default the
CPU count) and batch size are process-wide settings read when a table's
session is created, at its reader or constructor call (``read_parquet``,
``read_csv``, ``from_arrow``, ...). A table keeps the layout in effect at
that call through every later transform and execution, so pin the layout
before building the tables a check compares. The parallel Parquet fast path
follows the same pinned layout.

Tests use the ``engine_config`` and ``engine_layout`` fixtures of
``py-ltseq/tests/conftest.py``; benchmarks and subprocesses use
``pinned_engine`` directly. ``current_engine_config`` only reads the layout;
its name differs from the pinning ``engine_config`` fixture so that a test
taking the fixture can still call it.
"""

from collections.abc import Iterator
from contextlib import contextmanager

from . import ltseq_core


def current_engine_config() -> dict[str, int]:
    """The layout a table built now would get, and the defaults.

    Keys: ``target_partitions``, ``batch_size``,
    ``default_target_partitions`` (the CPU count the engine sees) and
    ``default_batch_size``.
    """
    return dict(ltseq_core._engine_config())


@contextmanager
def pinned_engine(
    *, target_partitions: int | None = None, batch_size: int | None = None
) -> Iterator[dict[str, int]]:
    """Pin the engine layout for tables built inside the block.

    ``None`` means the engine default. The values in effect before the block
    come back on exit, also when the block raises, so blocks nest. Yields
    the resulting ``current_engine_config()``. Raises ``ValueError`` for a
    value below 1.
    """
    previous = ltseq_core._set_layout_overrides(target_partitions, batch_size)
    try:
        yield current_engine_config()
    finally:
        ltseq_core._set_layout_overrides(*previous)
