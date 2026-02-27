"""Cursor: Streaming iterator for lazy batch-by-batch data processing.

This module provides a Python wrapper around LTSeqCursor for processing
large datasets without loading everything into memory.
"""

from typing import Dict

try:
    from . import ltseq_core
    HAS_RUST_BINDING = True
except ImportError:
    HAS_RUST_BINDING = False


class Cursor:
    """Lazy iterator over batches of data.

    Unlike LTSeq which loads all data into memory, Cursor streams data
    in batches, enabling processing of datasets larger than RAM.

    Example:
        >>> for batch in LTSeq.scan("large_file.csv"):
        ...     df = batch.to_pandas()
        ...     process(df)

        >>> # Or collect all at once (materializes to memory)
        >>> df = LTSeq.scan("file.csv").to_pandas()
    """

    def __init__(self, rust_cursor):
        """Initialize Cursor with a LTSeqCursor instance.

        Args:
            rust_cursor: LTSeqCursor from ltseq_core
        """
        if not HAS_RUST_BINDING:
            raise RuntimeError(
                "Rust extension ltseq_core not available. "
                "Please rebuild with `maturin develop`."
            )
        self._inner = rust_cursor
        self._schema = None  # Lazily populated

    def __iter__(self) -> "Cursor":
        """Return self as iterator."""
        return self

    def __next__(self):
        """Fetch the next batch.

        Returns:
            PyArrow RecordBatch

        Raises:
            StopIteration: When stream is exhausted
        """
        import pyarrow as pa

        batch_bytes = self._inner.next_batch()
        if batch_bytes is None:
            raise StopIteration

        # Deserialize IPC bytes to RecordBatch
        reader = pa.ipc.open_stream(batch_bytes)
        batch = reader.read_next_batch()
        return batch

    @property
    def schema(self) -> Dict[str, str]:
        """Get the schema as a dict of {column_name: data_type}."""
        if self._schema is None:
            self._schema = dict(self._inner.get_schema())
        return self._schema

    @property
    def columns(self) -> list:
        """Get column names."""
        return self._inner.get_column_names()

    @property
    def source(self) -> str:
        """Get the source file path."""
        return self._inner.get_source()

    @property
    def exhausted(self) -> bool:
        """Check if cursor is exhausted."""
        return self._inner.is_exhausted()

    def to_pandas(self):
        """Materialize all remaining batches into a pandas DataFrame.

        Warning: This loads all remaining data into memory.

        Returns:
            pandas.DataFrame
        """
        import pyarrow as pa

        batches = list(self)
        if not batches:
            # Return empty DataFrame with schema
            import pandas as pd
            return pd.DataFrame(columns=self.columns)

        # Combine all batches into a table, then convert to pandas
        table = pa.Table.from_batches(batches)
        return table.to_pandas()

    def to_arrow(self):
        """Materialize all remaining batches into a PyArrow Table.

        Warning: This loads all remaining data into memory.

        Returns:
            pyarrow.Table
        """
        import pyarrow as pa

        batches = list(self)
        if not batches:
            # Return empty table
            return pa.table({col: [] for col in self.columns})

        return pa.Table.from_batches(batches)

    def count(self) -> int:
        """Count all rows by iterating through the stream.

        Warning: This consumes the cursor.

        Returns:
            int: Total row count
        """
        total = 0
        for batch in self:
            total += batch.num_rows
        return total
