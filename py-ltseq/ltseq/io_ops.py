"""I/O operations for LTSeq: read_csv, write_csv, scan, _from_rows."""

from typing import Any, Dict

from .helpers import _infer_schema_from_csv, _infer_schema_from_parquet

try:
    from . import ltseq_core

    HAS_RUST_BINDING = True
except ImportError:
    HAS_RUST_BINDING = False


class IOMixin:
    """Mixin class providing I/O operations for LTSeq."""

    @classmethod
    def read_csv(cls, path: str, has_header: bool = True) -> "LTSeq":
        """
        Load a CSV file and return an LTSeq table.

        Args:
            path: Path to the CSV file
            has_header: Whether the first row is a header (default: True).
                       If False, columns are named column_0, column_1, etc.

        Returns:
            New LTSeq instance with loaded data

        Raises:
            FileNotFoundError: If path does not exist
            ValueError: If CSV parsing fails

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t_no_header = LTSeq.read_csv("data.csv", has_header=False)
        """
        from .core import LTSeq
        import os

        t = LTSeq()
        # Set table name from file basename (without extension) for lookup support
        t._name = os.path.splitext(os.path.basename(path))[0]
        t._inner.read_csv(path, has_header)
        t._schema = _infer_schema_from_csv(path, has_header)
        return t

    @classmethod
    def read_parquet(cls, path: str) -> "LTSeq":
        """
        Load a Parquet file and return an LTSeq table.

        Args:
            path: Path to the Parquet file

        Returns:
            New LTSeq instance with loaded data

        Raises:
            FileNotFoundError: If path does not exist
            RuntimeError: If Parquet parsing fails

        Example:
            >>> t = LTSeq.read_parquet("data.parquet")
        """
        from .core import LTSeq
        import os

        t = LTSeq()
        t._name = os.path.splitext(os.path.basename(path))[0]
        t._inner.read_parquet(path)
        t._schema = _infer_schema_from_parquet(path)
        return t

    @classmethod
    def scan(cls, path: str, has_header: bool = True) -> "Cursor":
        """
        Create a streaming cursor for a CSV file.

        Enables lazy iteration over large files without loading into memory.

        Args:
            path: Path to the CSV file
            has_header: Whether the first row is a header (default: True)

        Returns:
            Cursor for streaming iteration

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

        Args:
            path: Path to the Parquet file

        Returns:
            Cursor for streaming iteration

        Example:
            >>> cursor = LTSeq.scan_parquet("large.parquet")
            >>> for batch in cursor:
            ...     process(batch)
        """
        from .cursor import Cursor

        rust_cursor = ltseq_core.LTSeqTable.scan_parquet(path)
        return Cursor(rust_cursor)

    @classmethod
    def _from_rows(cls, rows: list[Dict[str, Any]], schema: Dict[str, str]) -> "LTSeq":
        """
        Create an LTSeq instance from a list of row dictionaries.

        Internal method used by partition() and similar operations.

        Args:
            rows: List of dictionaries, one per row
            schema: Column schema as {column_name: column_type_string}

        Returns:
            New LTSeq instance
        """
        import csv
        import os
        import tempfile

        from .core import LTSeq

        if not rows:
            # Create empty table with schema
            t = LTSeq()
            t._schema = schema
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, newline=""
            ) as f:
                writer = csv.DictWriter(f, fieldnames=schema.keys())
                writer.writeheader()
                temp_path = f.name
            try:
                t._inner.read_csv(temp_path, True)
            finally:
                os.unlink(temp_path)
            return t

        # Convert rows to CSV and load
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=schema.keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            temp_path = f.name

        t = LTSeq.read_csv(temp_path)
        return t

    def write_csv(self, path: str) -> None:
        """
        Write the table to a CSV file.

        Args:
            path: Path where the CSV file will be written

        Raises:
            RuntimeError: If write fails

        Example:
            >>> t.write_csv("output.csv")
        """
        self._inner.write_csv(path)
