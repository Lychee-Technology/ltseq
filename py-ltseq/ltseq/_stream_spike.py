"""#256's spike of the v0.5 stream error paths. Not public API.

Only an extension built with the ``stream_spike`` cargo feature carries the
Rust half (``maturin develop --features stream_spike``); shipped builds never
do, and ``available()`` says which this is. M1 replaces all of it with
``LTSeq.to_batches()`` and a ``__arrow_c_stream__`` that report errors as
contract §15.1 and §16.4 say. ``benchmarks/stream_errors/`` holds the probe,
the measurement and the report.

v0.5's exception classes (contract §20.1) do not exist yet, so this module
defines stand-ins with the contract's names and bases and hands them to the
Rust iterator by name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from . import ltseq_core

if TYPE_CHECKING:
    from . import LTSeq


class LTSeqError(Exception):
    pass


class LTSeqTypeError(LTSeqError, TypeError):
    pass


class LTSeqValueError(LTSeqError, ValueError):
    pass


class ColumnNotFoundError(LTSeqValueError, AttributeError):
    pass


class SchemaMismatchError(LTSeqValueError):
    pass


class SortRequiredError(LTSeqValueError):
    pass


class OrderViolationError(LTSeqValueError):
    pass


class DuplicateKeyError(LTSeqValueError):
    pass


class CastError(LTSeqValueError):
    pass


class LTSeqIndexError(LTSeqError, IndexError):
    pass


class ArithmeticOverflowError(LTSeqError, OverflowError):
    pass


class DivisionByZeroError(LTSeqError, ZeroDivisionError):
    pass


class LTSeqIOError(LTSeqError, OSError):
    pass


class SourceNotFoundError(LTSeqIOError, FileNotFoundError):
    pass


class ExecutionError(LTSeqError, RuntimeError):
    pass


CLASSES: dict[str, type[LTSeqError]] = {
    cls.__name__: cls
    for cls in (
        LTSeqTypeError,
        LTSeqValueError,
        ColumnNotFoundError,
        SchemaMismatchError,
        SortRequiredError,
        OrderViolationError,
        DuplicateKeyError,
        CastError,
        LTSeqIndexError,
        ArithmeticOverflowError,
        DivisionByZeroError,
        LTSeqIOError,
        SourceNotFoundError,
        ExecutionError,
    )
}

Fault = tuple[int, str, str]
"""``(after, class, message)``: raise ``class(message)`` instead of the
batch after the first ``after`` batches."""


def available() -> bool:
    """Whether this build carries the spike (cargo feature ``stream_spike``)."""
    return hasattr(ltseq_core, "_spike_batches")


def _core() -> Any:
    if not available():
        raise RuntimeError(
            "this ltseq build has no #256 stream spike; build it with "
            "`maturin develop --features stream_spike`"
        )
    return ltseq_core


def batches(table: LTSeq, fault: Fault | None = None, gil: str = "detach") -> Any:
    """Start an execution and return the Rust iterator over its batches.

    ``gil`` is how ``next()`` waits for a batch: ``"detach"`` releases the
    GIL for the pull (ADR 0016), ``"attached"`` keeps it (a lower bound for
    measurement only), ``"poll"`` takes a batch that is ready without
    releasing it and releases it only to wait.
    """
    return _core()._spike_batches(table._inner, CLASSES, fault, gil)


def to_batches(
    table: LTSeq, fault: Fault | None = None, gil: str = "detach"
) -> pa.RecordBatchReader:
    """The §15.1 candidate: a pyarrow reader over the Rust iterator."""
    it = batches(table, fault, gil)
    return pa.RecordBatchReader.from_batches(it.schema(), it)


class Stream:
    """An object whose ``__arrow_c_stream__`` is the §16.4 candidate.

    Each export starts a new execution, as ``LTSeq.__arrow_c_stream__`` does.
    ``requested_schema`` is accepted and ignored: the spike only concerns the
    stream's errors.
    """

    def __init__(self, table: LTSeq, fault: Fault | None = None) -> None:
        self._table = table
        self._fault = fault

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return _core()._spike_stream(self._table._inner, CLASSES, self._fault)


def live() -> int:
    """Executions a spike reader or stream holds now."""
    return _core()._spike_live()
