"""Row proxy for accessing columns of a row during predicate evaluation."""

from typing import Any


class RowProxy:
    """Proxy for accessing columns of a row during predicate evaluation."""

    def __init__(self, row_data: Any):
        """
        Initialize a row proxy.

        Args:
            row_data: A pandas Series, dict, or row-like object
        """
        self._row_data = row_data

    def __getattr__(self, col_name: str) -> Any:
        """Access a column value from the row."""
        if col_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{col_name}'"
            )

        # Handle dict-like access
        if isinstance(self._row_data, dict):
            if col_name in self._row_data:
                return self._row_data[col_name]
        # Handle pandas Series access
        elif hasattr(self._row_data, "__getitem__"):
            try:
                return self._row_data[col_name]
            except (KeyError, IndexError):
                pass

        raise AttributeError(f"Column '{col_name}' not found in row")
