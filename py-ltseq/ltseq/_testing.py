"""Test-only engine configuration (#274). Not public API.

The engine's parallelism (DataFusion ``target_partitions``, by default the
CPU count) and batch size are process-wide settings read when a table's
session is created, at its reader or constructor call (``read_parquet``,
``read_csv``, ``from_arrow``, ...). A table keeps the layout in effect at
that call through every later transform and execution, so pin the layout
before building the tables a check compares. Under a pin, the parallel
Parquet count runs at the layout ``current_engine_config()`` reports, a
value left at the default included; with nothing pinned it keeps its own
read size.

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


def prototypes_available() -> bool:
    """Whether this build carries the #251 measurement prototypes.

    Only an extension built with the ``m0`` cargo feature does (``maturin
    develop --release --features m0``); shipped builds never do.
    """
    return hasattr(ltseq_core, "_set_m0_prototypes")


@contextmanager
def prototypes(**flags: object) -> Iterator[dict[str, str]]:
    """Turn on #251's measurement prototypes for tables built inside the block.

    Every flag left out is off, so the block's prototypes are exactly the
    ones named; the flags in effect before come back on exit. Flags and
    values are documented in ``src/m0/mod.rs`` (for example
    ``checked="fast"``, ``order="interleaved"``, ``order_window=4``). A table
    keeps the prototypes in effect at its reader or constructor call, as it
    keeps the layout of ``pinned_engine``. Yields the flags now in effect.
    Raises ``RuntimeError`` on a build without the prototypes and
    ``ValueError`` for an unknown flag or value.
    """
    if not prototypes_available():
        raise RuntimeError(
            "this ltseq build has no #251 prototypes; build it with "
            "`maturin develop --release --features m0`"
        )
    core = ltseq_core
    previous = core._set_m0_prototypes(  # type: ignore[attr-defined]
        {name: _flag_text(value) for name, value in flags.items()}
    )
    try:
        yield dict(core._m0_prototypes())  # type: ignore[attr-defined]
    finally:
        core._set_m0_prototypes(previous)  # type: ignore[attr-defined]


def _flag_text(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
