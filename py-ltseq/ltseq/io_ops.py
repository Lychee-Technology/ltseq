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

    def write_parquet(self, path: str, compression: str | None = None) -> None:
        """
        Write the table to a Parquet file.

        Args:
            path: Path where the Parquet file will be written
            compression: Optional compression algorithm.
                         One of "snappy", "zstd", "gzip", "lz4", "none".
                         Defaults to uncompressed if not specified.

        Raises:
            RuntimeError: If write fails
            ValueError: If compression is not recognized

        Example:
            >>> t.write_parquet("output.parquet")
            >>> t.write_parquet("output.parquet", compression="zstd")
        """
        self._inner.write_parquet(path, compression)

    @classmethod
    def from_arrow(cls, arrow_table) -> "LTSeq":
        """
        Create an LTSeq table from a PyArrow Table.

        Args:
            arrow_table: A pyarrow.Table instance

        Returns:
            New LTSeq instance backed by the Arrow data

        Example:
            >>> import pyarrow as pa
            >>> arrow_table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> t = LTSeq.from_arrow(arrow_table)
        """
        import pyarrow as pa

        from .core import LTSeq

        if not isinstance(arrow_table, pa.Table):
            raise TypeError(
                f"Expected pyarrow.Table, got {type(arrow_table).__name__}"
            )

        # Serialize Arrow Table to IPC bytes
        batches = arrow_table.to_batches()
        if not batches and arrow_table.num_rows == 0:
            # Create an empty batch with the schema so Rust receives schema info
            empty_batch = pa.RecordBatch.from_pydict(
                {field.name: pa.array([], type=field.type) for field in arrow_table.schema},
                schema=arrow_table.schema,
            )
            batches = [empty_batch]
        ipc_buffers = []
        for batch in batches:
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            ipc_buffers.append(sink.getvalue().to_pybytes())

        # Load into Rust via the IPC pathway
        t = LTSeq()
        t._inner = ltseq_core.LTSeqTable.load_arrow_ipc(ipc_buffers)

        # Infer schema from Arrow schema
        _arrow_to_ltseq_type = {
            "int8": "int64",
            "int16": "int64",
            "int32": "int64",
            "int64": "int64",
            "uint8": "int64",
            "uint16": "int64",
            "uint32": "int64",
            "uint64": "int64",
            "float16": "float64",
            "float32": "float64",
            "float64": "float64",
            "double": "float64",
            "bool": "bool",
            "string": "string",
            "large_string": "string",
            "utf8": "string",
            "large_utf8": "string",
            "date32": "date32",
            "date64": "date32",
        }
        schema = {}
        for field in arrow_table.schema:
            type_str = str(field.type)
            schema[field.name] = _arrow_to_ltseq_type.get(type_str, type_str)
        t._schema = schema

        return t

    @classmethod
    def from_pandas(cls, df) -> "LTSeq":
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
