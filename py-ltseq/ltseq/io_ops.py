"""I/O operations for LTSeq: read_csv, write_csv, scan, from_rows, from_dict, _from_rows."""

from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .core import LTSeq
    from .cursor import Cursor

# Note: _infer_schema_from_csv and _infer_schema_from_parquet are no longer used;
# schema is now obtained from Rust via get_schema_dict() during file reads.


def _infer_schema_from_rows(sample_row: dict[str, Any]) -> dict[str, str]:
    """Infer Arrow type strings from a sample row's Python values."""
    schema = {}
    for col, val in sample_row.items():
        if isinstance(val, bool):
            schema[col] = "Boolean"
        elif isinstance(val, int):
            schema[col] = "Int64"
        elif isinstance(val, float):
            schema[col] = "Float64"
        else:
            schema[col] = "Utf8"
    return schema

try:
    from . import ltseq_core

    HAS_RUST_BINDING = True
except ImportError:
    ltseq_core = cast(Any, None)
    HAS_RUST_BINDING = False


class IOMixin:
    """Mixin class providing I/O operations for LTSeq."""

    _inner: Any

    @classmethod
    def read_csv(cls, path: str, has_header: bool = True) -> "LTSeq":
        """
        Load a CSV file and return an LTSeq table.

        `path` is a file, a directory, whose `.csv` files are read, or a
        glob. An existing file or directory is read as itself, even when its
        name holds `*`, `?` or `[`; any other path that holds them is a glob,
        which reads the `.csv` files it matches. `*`, `?` and `[...]` match
        within one path segment, `**` across directories, and a name that
        starts with `.` only by a literal `.`, as in Python's
        `glob.glob(path, recursive=True)`. A directory the glob has to list
        but cannot read raises, where Python's glob skips it.

        The table reads the files found at the call, so a file added to the
        directory later, or matched by the glob later, is not read. Each
        execution reads those files as they are then, and raises if one of
        them no longer exists.

        Args:
            path: Path to a CSV file, a directory or a glob
            has_header: Whether the first row is a header (default: True).
                       If False, columns are named column_0, column_1, etc.

        Returns:
            New LTSeq instance with loaded data

        Raises:
            FileNotFoundError: If `path` names no file: it does not exist, or
                it is a directory or a glob without a `.csv` file. The
                message names `path`.
            ValueError: If CSV parsing fails

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t_no_header = LTSeq.read_csv("data.csv", has_header=False)
            >>> t_days = LTSeq.read_csv("logs/2024-*.csv")
        """
        from .core import LTSeq
        import os

        # Use Rust static constructor to load CSV directly without empty init overhead
        inner = ltseq_core.LTSeqTable.from_csv(path, has_header)
        
        # Wrap in Python layer (schema is fetched lazily from Rust)
        t = LTSeq._from_inner(inner)
        t._name = os.path.splitext(os.path.basename(path))[0]
        return t

    @classmethod
    def read_parquet(cls, path: str) -> "LTSeq":
        """
        Load a Parquet file and return an LTSeq table.

        `path` is a file, a directory, whose `.parquet` files are read, or a
        glob, which reads the `.parquet` files it matches. It resolves as in
        `read_csv`.

        The table reads the files found at the call, so a file added to the
        directory later, or matched by the glob later, is not read. Each
        execution reads those files as they are then, their footers
        included, and raises if one of them no longer exists.

        Args:
            path: Path to a Parquet file, a directory or a glob

        Returns:
            New LTSeq instance with loaded data

        Raises:
            FileNotFoundError: If `path` names no file: it does not exist, or
                it is a directory or a glob without a `.parquet` file. The
                message names `path`.
            RuntimeError: If Parquet parsing fails

        Example:
            >>> t = LTSeq.read_parquet("data.parquet")
            >>> t_parts = LTSeq.read_parquet("data/part-*.parquet")
        """
        from .core import LTSeq
        import os

        # Use Rust static constructor to load Parquet directly without empty init overhead
        inner = ltseq_core.LTSeqTable.from_parquet(path)
        
        # Wrap in Python layer (schema is fetched lazily from Rust)
        t = LTSeq._from_inner(inner)
        t._name = os.path.splitext(os.path.basename(path))[0]
        return t

    @classmethod
    def scan(cls, path: str, has_header: bool = True) -> "Cursor":
        """
        Create a streaming cursor for a CSV file.

        Enables lazy iteration over large files without loading into memory.
        `path` resolves as in `read_csv`.

        Args:
            path: Path to a CSV file, a directory or a glob
            has_header: Whether the first row is a header (default: True)

        Returns:
            Cursor for streaming iteration

        Raises:
            FileNotFoundError: If `path` names no file, as in `read_csv`

        Example:
            >>> cursor = LTSeq.scan("large.csv")
            >>> for batch in cursor:
            ...     process(batch)
        """
        from .cursor import Cursor

        rust_cursor = ltseq_core.LTSeqTable.scan_csv(path, has_header)
        return Cursor(rust_cursor)

    @classmethod
    def scan_parquet(cls, path: str) -> "Cursor":
        """
        Create a streaming cursor for a Parquet file.

        `path` resolves as in `read_parquet`.

        Args:
            path: Path to a Parquet file, a directory or a glob

        Returns:
            Cursor for streaming iteration

        Raises:
            FileNotFoundError: If `path` names no file, as in `read_parquet`

        Example:
            >>> cursor = LTSeq.scan_parquet("large.parquet")
            >>> for batch in cursor:
            ...     process(batch)
        """
        from .cursor import Cursor

        rust_cursor = ltseq_core.LTSeqTable.scan_parquet(path)
        return Cursor(rust_cursor)

    @classmethod
    def from_rows(
        cls, rows: list[dict[str, Any]], schema: dict[str, str] | None = None
    ) -> "LTSeq":
        """
        Create an LTSeq table from a list of row dictionaries.

        Args:
            rows: List of dicts, one per row. All dicts must have the same keys.
            schema: Optional column schema as {column_name: arrow_type_string}.
                    If None, types are inferred from the first row's values.
                    Explicit schema is required for empty rows lists.

        Returns:
            New LTSeq instance

        Raises:
            ValueError: If rows is empty and schema is None
            TypeError: If rows is not a list of dicts

        Example:
            >>> t = LTSeq.from_rows([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
            >>> t = LTSeq.from_rows([], schema={"id": "Int64", "name": "Utf8"})
        """
        if schema is None:
            if not rows:
                raise ValueError(
                    "from_rows() requires an explicit schema when rows is empty. "
                    "Use from_rows([], schema={'col': 'Int64', ...})"
                )
            schema = _infer_schema_from_rows(rows[0])
        return cls._from_rows(rows, schema)

    @classmethod
    def from_dict(cls, data: dict[str, list[Any]]) -> "LTSeq":
        """
        Create an LTSeq table from a column-oriented dictionary.

        Args:
            data: Dict mapping column names to lists of values.
                  All lists must have the same length.

        Returns:
            New LTSeq instance

        Raises:
            ValueError: If column lists have different lengths
            TypeError: If data is not a dict

        Example:
            >>> t = LTSeq.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        """
        if not isinstance(data, dict):
            raise TypeError(f"from_dict() expects a dict, got {type(data).__name__}")

        if not data:
            raise ValueError("from_dict() requires at least one column")

        lengths = [len(v) for v in data.values()]
        if len(set(lengths)) > 1:
            raise ValueError(
                f"All column lists must have the same length. "
                f"Got lengths: { {k: len(v) for k, v in data.items()} }"
            )

        n = lengths[0]
        cols = list(data.keys())
        rows = [{col: data[col][i] for col in cols} for i in range(n)]
        return cls.from_rows(rows)

    @classmethod
    def _from_rows(cls, rows: list[dict[str, Any]], schema: dict[str, str]) -> "LTSeq":
        """
        Create an LTSeq instance from a list of row dictionaries via a pyarrow Table.

        Internal method used by partition() and similar operations.
        Callers should prefer the public from_rows() which supports schema inference.

        Args:
            rows: List of dictionaries, one per row
            schema: Column schema as {column_name: column_type_string}

        Returns:
            New LTSeq instance
        """
        import pyarrow as pa

        from .core import LTSeq

        # Map LTSeq schema type strings to PyArrow types (for empty-table creation)
        _schema_to_arrow: dict[str, Any] = {
            "int64": pa.int64(), "Int64": pa.int64(),
            "float64": pa.float64(), "Float64": pa.float64(),
            "bool": pa.bool_(), "Boolean": pa.bool_(),
            "string": pa.string(), "Utf8": pa.string(), "utf8": pa.string(),
            "large_string": pa.large_utf8(), "large_utf8": pa.large_utf8(),
            "date32": pa.date32(), "date64": pa.date64(),
        }

        col_names = list(schema.keys())

        if not rows:
            arrays = {
                col: pa.array([], type=_schema_to_arrow.get(schema[col], pa.string()))
                for col in col_names
            }
            pa_table = pa.table(arrays)
        else:
            col_data = {col: [row.get(col) for row in rows] for col in col_names}
            pa_table = pa.table(col_data)

        # Rust's Arrow-derived schema is authoritative; the Python-side
        # inference above is only used to build the Arrow table.
        return LTSeq.from_arrow(pa_table)

    def write_csv(self, path: str) -> None:
        """
        Write the table to a CSV file.

        When ``path`` is a regular file or does not exist yet, the rows go to
        a hidden temporary file (``.ltseq-*.tmp``) in the same directory,
        which is flushed to disk and renamed over ``path`` only after the
        whole table is written. If the write fails, a file already at
        ``path`` is left unchanged and the temporary file is removed. A
        symlink at ``path`` is followed: its target is replaced, or created
        if it does not exist yet, and the link stays.

        What a rename cannot replace is written directly, with no such
        guarantee: a failed write can leave part of the output there. That
        is a device or a pipe (``/dev/null``, ``/dev/stdout`` on a terminal
        or a pipe), a deleted file reached through ``/proc/self/fd/N`` on
        Linux, and on macOS any ``/dev/fd/N``, so ``/dev/stdout`` redirected
        to a file is written in place there. On Linux that file is replaced.
        A file that is itself a mount point, such as a Docker single-file
        bind mount, cannot be renamed over either, but is not written
        directly: the write fails once the whole table is written, and the
        file is left as it was. Mount its directory instead.

        The guarantee's limits:

        - Readers see the old file or the new one, never part of one. After
          a crash or a power loss ``path`` holds the old contents or the new
          ones in full, provided the filesystem keeps its flush promise; the
          rename itself is not flushed, so a write that had returned can
          read as the old file after a power loss. A process killed during
          the write leaves its temporary file behind.
        - ``path`` is resolved when the call starts. Renaming or replacing
          the destination's directory, retargeting a link on the way, or
          taking away write access to the directory while the write runs
          can make it fail and leave the temporary file behind, or put the
          file where the link pointed at the start.
        - The file is replaced, not rewritten. It keeps its mode, its group
          and its ACL (POSIX on Linux, extended on macOS); a writer who
          cannot set that group gets a file only its owner can open. It is
          owned by the writer and loses other extended attributes, hard
          links to it keep the old contents, and replacing it needs write
          access to its directory.

        Args:
            path: Path where the CSV file will be written

        Raises:
            RuntimeError: If write fails

        Example:
            >>> t.write_csv("output.csv")
        """
        self._inner.write_csv(path)

    def write_parquet(self, path: str, compression: str | None = None) -> None:
        """
        Write the table to a Parquet file.

        When ``path`` is a regular file or does not exist yet, the rows go to
        a hidden temporary file (``.ltseq-*.tmp``) in the same directory,
        which is flushed to disk and renamed over ``path`` only after the
        whole table is written. If the write fails, a file already at
        ``path`` is left unchanged and the temporary file is removed. Until
        the rename the old file stays in place, so the table may read
        ``path`` itself. A symlink at ``path`` is followed: its target is
        replaced, or created if it does not exist yet, and the link stays.

        A device, a pipe or a descriptor path that a rename cannot replace
        is written directly, with no such guarantee: a failed write can
        leave part of the output there. :meth:`write_csv` lists those
        paths, and the guarantee's limits.

        Parquet has no second unit, so a ``timestamp`` or ``time32`` in
        seconds is written in milliseconds, as pyarrow writes it, also
        inside a list, struct, map, dictionary or run-end encoded column.
        Readers that go by Parquet's types, such as pandas and DuckDB, read
        it as a time, and :meth:`read_parquet` reads it back as
        ``timestamp[ms]`` or ``time32[ms]``, with the same values and time
        zone. A ``timestamp[s]`` value beyond ±9223372036854775 seconds
        (about 292 million years from 1970) has no millisecond count in
        int64 and fails the write.

        Args:
            path: Path where the Parquet file will be written
            compression: Optional compression algorithm.
                         One of "snappy", "zstd", "gzip", "lz4", "none".
                         Defaults to uncompressed if not specified.

        Raises:
            RuntimeError: If write fails
            ValueError: If compression is not recognized, or if a
                ``timestamp[s]`` value has no millisecond count in int64;
                ``path`` is left as it was
            TypeError: If a column is or contains a ``struct`` without fields
                or a ``fixed_size_binary`` of width 0, which Parquet cannot
                store; raised before anything is written

        Example:
            >>> t.write_parquet("output.parquet")
            >>> t.write_parquet("output.parquet", compression="zstd")
        """
        self._inner.write_parquet(path, compression)

    @classmethod
    def from_arrow(cls, arrow_table: Any) -> "LTSeq":
        """
        Create an LTSeq table from Arrow data.

        Accepts any object implementing the Arrow PyCapsule Interface
        (``__arrow_c_stream__``): a ``pyarrow.Table``, ``pyarrow.RecordBatch``
        or ``pyarrow.RecordBatchReader``, a polars DataFrame, a duckdb
        relation, .... The buffers are shared over the Arrow C Data Interface,
        not copied, and stay valid after the source object is released.

        A struct-typed ``pyarrow.ChunkedArray`` is read as the table of its
        fields. A row that is NULL as a whole cannot be a table row, so it
        raises; ``pyarrow.Table.from_struct_array`` reads its fields as NULL.

        Args:
            arrow_table: An Arrow-compatible object

        Returns:
            New LTSeq instance backed by the Arrow data

        Raises:
            TypeError: If the object does not implement ``__arrow_c_stream__``
            ValueError: If a row of a struct input is NULL as a whole, or the
                stream fails or does not describe a table

        Example:
            >>> import pyarrow as pa
            >>> arrow_table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> t = LTSeq.from_arrow(arrow_table)
        """
        from .core import LTSeq

        if not hasattr(arrow_table, "__arrow_c_stream__"):
            raise TypeError(
                "Expected pyarrow.Table or another Arrow-compatible object implementing "
                "__arrow_c_stream__ (pyarrow.RecordBatch, pyarrow.RecordBatchReader, ...), "
                f"got {type(arrow_table).__name__}"
            )

        inner = ltseq_core.LTSeqTable.from_arrow(arrow_table)
        return LTSeq._from_inner(inner)

    @classmethod
    def from_pandas(cls, df: Any) -> "LTSeq":
        """
        Create an LTSeq table from a pandas DataFrame.

        Converts the DataFrame to a PyArrow Table first, then loads it.

        Args:
            df: A pandas DataFrame

        Returns:
            New LTSeq instance backed by the DataFrame data

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> t = LTSeq.from_pandas(df)
        """
        import pyarrow as pa

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "from_pandas() requires pandas. Install it with: pip install pandas"
            )

        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Expected pandas.DataFrame, got {type(df).__name__}"
            )

        arrow_table = pa.Table.from_pandas(df)
        return cls.from_arrow(arrow_table)
