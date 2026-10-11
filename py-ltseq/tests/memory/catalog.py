"""Pipelines and draining terminals the memory harness runs (contract §21.1).

A pipeline maps the table its reader returned to the table a terminal
drains; ``read`` builds another table over the same source, for operations
with a second input. A terminal drains a table the way §21.1 bounds it and
returns the rows it saw, or the path of the file it wrote, which the child
counts after the measurement. Both run in the child process inside the
measured window. The input columns are those of ``harness.input_rows``.

The names follow v0.5, and some entries stand in for it until the M items
land: v0.4 has no ``to_batches`` (M1), so that terminal drains LTSeq's side
of the Arrow C stream, which is what ``to_batches`` will return (§15.1).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pyarrow as pa

from ltseq import LTSeq
from ltseq.expr.base import row_number

# A MUST-stream pipeline of §21.1: a reader, then filter, select and derive.
# It keeps three rows of every four, and leaves `key`, `run` and `w` behind.
MUST = {
    "filter_select_derive": lambda t, read: (
        t.filter(lambda r: r.id % 4 != 0).select("id", "v").derive(x=lambda r: r.v * 2)
    ),
}

# The harness's negative control: `collect` holds the whole input.
MATERIALIZING = {
    "collect": lambda t, read: t.collect(),
}


def _asof(t: LTSeq, read: Callable[[], LTSeq]) -> LTSeq:
    right = read().select("id", "key", "w").rename(id="right_id").assume_sorted("right_id")
    return t.assume_sorted("id").asof_join(
        right, left_on="id", right_on="right_id", by="key", is_sorted=True
    )


# The operations §21.1 says SHOULD hold only what their definition needs.
# Over this input that is a few KiB: 8 keys, 1,024-row runs and windows of
# at most 16 rows.
SHOULD = {
    "shift": lambda t, read: t.assume_sorted("id").derive(lambda r: {"p": r.v.shift(1)}),
    "shift_by_key": lambda t, read: (
        t.assume_sorted("id").derive(lambda r: {"p": r.v.shift(1, partition_by="key")})
    ),
    "rolling": lambda t, read: (
        t.assume_sorted("id").derive(lambda r: {"m": r.v.rolling(16).mean()})
    ),
    "cum_sum": lambda t, read: t.assume_sorted("id").derive(lambda r: {"c": r.v.cum_sum()}),
    "row_number": lambda t, read: (
        t.assume_sorted("id").derive(lambda r: {"n": row_number().over()})
    ),
    "group_ordered": lambda t, read: (
        t.assume_sorted("id")
        .group_ordered(lambda r: r.run)
        .agg(lambda g: {"n": g.count(), "first": g.first().id})
    ),
    "search_pattern": lambda t, read: (
        t.assume_sorted("id").search_pattern(lambda r: r.key == 0, lambda r: r.key == 1)
    ),
    "asof_join": _asof,
    "distinct": lambda t, read: t.distinct("key"),
}

# Owner decision U2 (#278) waits for these measurements: whether `tail(n)`
# gets a SHOULD bound (it MAY hold the whole input today) and whether
# `distinct` moves into the MUST-stream list.
U2 = {
    "tail": lambda t, read: t.tail(10),
}

PIPELINES = {**MUST, **MATERIALIZING, **SHOULD, **U2}


def _to_batches(table: LTSeq, out_dir: Path) -> int:
    # M1 replaces the stream reader with `table.to_batches()`.
    rows = 0
    for batch in pa.RecordBatchReader.from_stream(table):
        rows += batch.num_rows
    return rows


def _iterate(table: LTSeq, out_dir: Path) -> int:
    rows = 0
    for _ in table:
        rows += 1
    return rows


def _write_parquet(table: LTSeq, out_dir: Path) -> Path:
    path = out_dir / "out.parquet"
    table.write_parquet(str(path))
    return path


def _write_csv(table: LTSeq, out_dir: Path) -> Path:
    path = out_dir / "out.csv"
    table.write_csv(str(path))
    return path


# The draining terminals of R3, each dropping what it read.
TERMINALS: dict[str, Callable[[LTSeq, Path], int | Path]] = {
    "to_batches": _to_batches,
    "iterate": _iterate,
    "write_parquet": _write_parquet,
    "write_csv": _write_csv,
    "count": lambda table, out_dir: table.count(),
}
