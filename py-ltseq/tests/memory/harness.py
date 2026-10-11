"""Peak-memory harness for row R3 of the v0.5 contract (§21.1, §24.3; #274).

R3: each MUST-stream pipeline, over an input at least ten times a fixed
budget and drained by each draining terminal, keeps the process's peak
memory, above what it held before the reader's call, within the budget.
``measure`` runs one (pipeline, terminal) pair of ``catalog`` in a fresh
process (``child.py``), so the peak belongs to that run alone.

§21.1 bounds memory for a fixed partition count and batch size, so the
harness pins both: two partitions and the engine's default batch size. The
peak grows with the partition count, so the default, the CPU count, would
make the figure depend on the machine: over this input, ``write_parquet`` of
the MUST pipeline in ``catalog`` peaks at 40 MiB at two partitions and
102 MiB at twelve, a 12-CPU machine's default.

Linux only: the child resets the kernel's peak counter, which no other
platform offers.
"""

from __future__ import annotations

import json
import math
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .. import layouts

MiB = 2**20
BUDGET = 64 * MiB
TARGET_PARTITIONS = 2
# The child stops a run whose resident size grows past this, which is
# already a failure, so a materializing run cannot exhaust the machine.
ABORT_ABOVE = 4 * BUDGET
SUPPORTED = sys.platform == "linux"

PY_LTSEQ = Path(__file__).resolve().parents[2]


class BudgetExceeded(AssertionError):
    """A run's peak memory went over its budget."""


class ShouldBoundExceeded(UserWarning):
    """A §21.1 SHOULD operation held more than its definition needs."""


@dataclass(frozen=True)
class Source:
    """An input the child reads: ``read_parquet(path)`` or ``read_csv(path)``."""

    format: str
    path: Path
    rows: int
    nbytes: int  # decoded Arrow size of the rows


@dataclass(frozen=True)
class Measurement:
    pipeline: str
    terminal: str
    budget: int
    peak_above: int
    aborted: bool
    drained: int | None
    seconds: float | None

    @property
    def within_budget(self) -> bool:
        return not self.aborted and self.peak_above <= self.budget

    def describe(self) -> str:
        peak = f"{self.peak_above / MiB:.1f} MiB"
        if self.aborted:
            peak = f"> {peak} (stopped)"
        verdict = "within" if self.within_budget else "OVER"
        return (
            f"{self.pipeline} -> {self.terminal}: peak {peak}, {verdict} the "
            f"{self.budget / MiB:.0f} MiB budget"
        )


# Every measurement of the session, for the summary `conftest.py` prints.
MEASUREMENTS: list[Measurement] = []


def input_rows(start: int, stop: int) -> pa.Table:
    """Rows ``start`` to ``stop`` of the harness input.

    ``id`` is the row's position, and so the file and row order; ``key``
    cycles through 8 values; ``run`` changes every 1,024 rows; ``v`` and
    ``w`` are a float and an integer payload.
    """
    ids = np.arange(start, stop, dtype=np.int64)
    return pa.table(
        {
            "id": ids,
            "key": ids % 8,
            "run": ids // 1024,
            "v": ids * 0.5,
            "w": (ids * 7919) % 104729,
        }
    )


ROW_BYTES = input_rows(0, 1).nbytes


def rows_for(budget: int, *, files: int) -> int:
    """The fewest rows, in whole fours per file, decoding to ten budgets."""
    unit = 4 * files
    return math.ceil(10 * budget / ROW_BYTES / unit) * unit


def write_input(directory: Path, rows: int, *, files: int = 8) -> Source:
    """Write ``rows`` rows of ``input_rows`` as ``files`` Parquet files.

    Files are written one at a time, each with row groups of 128 Ki rows,
    and encoded so that the files stay small while the decoded input is
    large.
    """
    paths = layouts.part_paths(directory, files, "parquet")
    per_file = math.ceil(rows / files)
    nbytes = 0
    for index, path in enumerate(paths):
        part = input_rows(index * per_file, min(rows, (index + 1) * per_file))
        nbytes += part.nbytes
        pq.write_table(
            part,
            path,
            row_group_size=128 * 1024,
            use_dictionary=False,
            column_encoding={
                "id": "DELTA_BINARY_PACKED",
                "run": "DELTA_BINARY_PACKED",
                "v": "BYTE_STREAM_SPLIT",
                "w": "DELTA_BINARY_PACKED",
            },
        )
    return Source("parquet", directory, rows, nbytes)


def measure(
    source: Source,
    pipeline: str,
    terminal: str,
    *,
    warmup: Source,
    out_dir: Path,
    budget: int = BUDGET,
    target_partitions: int = TARGET_PARTITIONS,
    batch_size: int | None = None,
    timeout: float = 1800,
) -> Measurement:
    """Run ``pipeline`` and ``terminal`` of ``catalog`` over ``source`` in a
    fresh process and return its peak memory above its level before the
    reader's call. ``warmup`` is a small input of the same schema the child
    runs first. A child that fails raises ``RuntimeError``; a run over its
    budget is a measurement like any other."""
    if source.nbytes < 10 * budget:
        raise ValueError(
            f"R3 needs an input of at least ten budgets: {source.nbytes} bytes "
            f"against a {budget}-byte budget"
        )
    spec = {
        # The child runs in py-ltseq/, so relative paths would not resolve.
        "source": {"format": source.format, "path": str(source.path.resolve())},
        "warmup": {"format": warmup.format, "path": str(warmup.path.resolve())},
        "pipeline": pipeline,
        "terminal": terminal,
        "out_dir": str(out_dir.resolve()),
        "target_partitions": target_partitions,
        "batch_size": batch_size,
        "abort_above": max(ABORT_ABOVE, 2 * budget),
    }
    try:
        process = subprocess.run(
            [sys.executable, "-m", "tests.memory.child", json.dumps(spec)],
            cwd=PY_LTSEQ,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)
    lines = process.stdout.strip().splitlines()
    if process.returncode != 0 or not lines:
        raise RuntimeError(
            f"memory child for {pipeline} -> {terminal} failed "
            f"(exit {process.returncode}):\n{process.stderr[-4000:]}"
        )
    result = json.loads(lines[-1])
    measurement = Measurement(
        pipeline=pipeline,
        terminal=terminal,
        budget=budget,
        peak_above=result["peak_above"],
        aborted=result["aborted"],
        drained=result["drained"],
        seconds=result.get("seconds"),
    )
    MEASUREMENTS.append(measurement)
    return measurement


def assert_within_budget(measurement: Measurement) -> None:
    if not measurement.within_budget:
        raise BudgetExceeded(measurement.describe())
