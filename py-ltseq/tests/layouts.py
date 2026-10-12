"""Engine and input layouts for layout-independent tests (#274).

Results the v0.5 contract fixes must not depend on the partition count, the
batch size or how the input is split (§9.1, §20.2, §21.2), and the default
partition count, the CPU count, hides such a dependence on small machines
(#148). The fixtures of ``conftest.py`` sweep both:

- Engine layouts are the (partition count, batch size) pairs that
  ``engine_layout`` pins: 1, 2 and twice the CPU count the engine sees,
  crossed with 1, 7 and the engine default.
- Input layouts are the same rows as one or many batches, files and row
  groups. ``data_layout`` reads a test's rows back through ``build`` in each
  layout of ``LAYOUTS``.

A "many" layout splits the rows into ``MANY`` contiguous parts (fewer when
there are fewer rows): two partitions then hold several parts each, and
twice the CPU count of any machine with two or more CPUs exceeds the part
count. Files are named so that path order is row order (``part-00000``,
``part-00001``, ...), the file order v0.5 readers define (§4.1).

CSV files go through DataFusion's type inference on read, so CSV layouts
give back the same values only for columns it infers as their own type:
``int64``, finite ``float64``, ``bool`` and strings, with a non-NULL value
in every file (a column with no value in a file infers the null type).
Parquet strings come back as ``string_view`` until M18 normalizes views;
compare through ``plain_types``.
"""

from __future__ import annotations

import math
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from ltseq import LTSeq
from ltseq._testing import current_engine_config

CPUS = current_engine_config()["default_target_partitions"]
PARTITION_COUNTS = {"p1": 1, "p2": 2, "p2xcpu": 2 * CPUS}
BATCH_SIZES = {"b1": 1, "b7": 7, "bdefault": None}
ENGINE_LAYOUTS = {
    f"{p_id}-{b_id}": (partitions, batch_size)
    for p_id, partitions in PARTITION_COUNTS.items()
    for b_id, batch_size in BATCH_SIZES.items()
}

MANY = 3

LAYOUTS = (
    "one_batch",
    "many_batches",
    "parquet_file",
    "parquet_row_groups",
    "parquet_files",
    "csv_file",
    "csv_files",
)


def build(layout: str, rows: pa.Table, directory: Path) -> LTSeq:
    """Read ``rows`` back as a table in ``layout``.

    File layouts write under ``directory``, which must be empty or absent.
    The table is built under the engine layout in effect at this call.
    """
    rows = rows.combine_chunks()
    if layout == "one_batch":
        return LTSeq.from_arrow(rows)
    if layout == "many_batches":
        return LTSeq.from_arrow(pa.Table.from_batches(_batches(rows), rows.schema))
    if layout == "parquet_file":
        return LTSeq.read_parquet(str(write_parquet(rows, directory, files=1)))
    if layout == "parquet_row_groups":
        return LTSeq.read_parquet(
            str(write_parquet(rows, directory, files=1, row_groups=MANY))
        )
    if layout == "parquet_files":
        return LTSeq.read_parquet(str(write_parquet(rows, directory, files=MANY)))
    if layout == "csv_file":
        return LTSeq.read_csv(str(write_csv(rows, directory, files=1)))
    if layout == "csv_files":
        return LTSeq.read_csv(str(write_csv(rows, directory, files=MANY)))
    raise ValueError(f"unknown layout {layout!r}; expected one of {LAYOUTS}")


def write_parquet(
    rows: pa.Table, directory: Path, *, files: int, row_groups: int = 1, **options
) -> Path:
    """Write ``rows`` as ``files`` Parquet files of ``row_groups`` row groups each.

    Returns the single file's path for ``files=1``, else ``directory``, the
    form ``read_parquet`` takes for many files. ``options`` go to
    ``pyarrow.parquet.write_table``.
    """
    parts = split(rows, files)
    paths = part_paths(directory, len(parts), "parquet")
    for part, path in zip(parts, paths):
        row_group_size = max(1, math.ceil(part.num_rows / row_groups))
        pq.write_table(part, path, row_group_size=row_group_size, **options)
    return paths[0] if files == 1 else directory


def write_csv(rows: pa.Table, directory: Path, *, files: int) -> Path:
    """Write ``rows`` as ``files`` CSV files with a header each.

    Returns the single file's path for ``files=1``, else ``directory``.
    """
    parts = split(rows, files)
    paths = part_paths(directory, len(parts), "csv")
    for part, path in zip(parts, paths):
        pacsv.write_csv(_floats_as_text(part), path)
    return paths[0] if files == 1 else directory


def split(rows: pa.Table, parts: int) -> list[pa.Table]:
    """Cut ``rows`` into at most ``parts`` contiguous, non-empty slices.

    The earlier slices are the longer ones. Zero rows give one empty slice,
    so a file layout still writes a file that carries the schema.
    """
    size = max(1, math.ceil(rows.num_rows / parts))
    slices = [rows.slice(start, size) for start in range(0, rows.num_rows, size)]
    return slices or [rows]


def plain_types(table: pa.Table) -> pa.Table:
    """``table`` with view types cast to their plain counterparts."""
    views = {pa.string_view(): pa.string(), pa.binary_view(): pa.binary()}
    schema = pa.schema(
        field.with_type(views.get(field.type, field.type)) for field in table.schema
    )
    return table.cast(schema)


def part_paths(directory: Path, count: int, suffix: str) -> list[Path]:
    """Paths of ``count`` part files in ``directory``, in row order."""
    directory.mkdir(parents=True, exist_ok=True)
    return [directory / f"part-{index:05d}.{suffix}" for index in range(count)]


def _floats_as_text(rows: pa.Table) -> pa.Table:
    # pyarrow writes 1.0 as "1", which DataFusion infers as an integer, and a
    # file whose floats are all whole then fails to merge with the others.
    # Python's repr keeps the decimal point or exponent.
    def text(column: pa.ChunkedArray) -> pa.ChunkedArray | pa.Array:
        if not pa.types.is_floating(column.type):
            return column
        return pa.array(
            [None if value is None else repr(value) for value in column.to_pylist()],
            pa.string(),
        )

    return pa.table([text(column) for column in rows.columns], names=rows.column_names)


def _batches(rows: pa.Table) -> list[pa.RecordBatch]:
    size = max(1, math.ceil(rows.num_rows / MANY))
    return rows.to_batches(max_chunksize=size) or [
        pa.RecordBatch.from_pylist([], schema=rows.schema)
    ]
