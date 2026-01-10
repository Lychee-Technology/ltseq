"""Proxy classes for group and row access in NestedTable operations."""

from typing import TYPE_CHECKING, Any, Callable, List, Union

if TYPE_CHECKING:
    from .nested_table import NestedTable


class RowProxy:
    """Proxy for accessing columns of a row during predicate evaluation."""

    def __init__(self, row_data):
        """
        Initialize a row proxy.

        Args:
            row_data: A pandas Series, dict, or row-like object
        """
        self._row_data = row_data

    def __getattr__(self, col_name: str):
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


class GroupProxy:
    """
    Proxy for evaluating predicates on a group during filter operations.

    Provides access to group properties like count(), first(), last()
    so that predicates can be evaluated.
    """

    def __init__(self, group_data, nested_table: "NestedTable"):
        """
        Initialize a group proxy.

        Args:
            group_data: DataFrame containing rows for this group (list of dicts or pandas DataFrame)
            nested_table: The NestedTable this group belongs to
        """
        self._group_data = group_data
        self._nested_table = nested_table

    def count(self) -> int:
        """Get the number of rows in this group."""
        if isinstance(self._group_data, list):
            return len(self._group_data)
        else:
            return len(self._group_data)

    def first(self) -> RowProxy:
        """Get the first row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            first_row_data = self._group_data[0]
        else:
            first_row_data = self._group_data.iloc[0]
        return RowProxy(first_row_data)

    def last(self) -> RowProxy:
        """Get the last row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            last_row_data = self._group_data[-1]
        else:
            last_row_data = self._group_data.iloc[-1]
        return RowProxy(last_row_data)

    def max(self, column_name: str = None) -> Any:
        """
        Get the maximum value in a column for this group.

        Args:
            column_name: Name of the column to find max for.
                        If not provided, returns self for chaining.

        Returns:
            The maximum value in the column for this group.

        Example:
            >>> g.max('price')  # Max price in group
        """
        if column_name is None:
            return self

        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        valid_values = [v for v in values if v is not None]
        return max(valid_values) if valid_values else None

    def min(self, column_name: str = None) -> Any:
        """
        Get the minimum value in a column for this group.

        Args:
            column_name: Name of the column to find min for.
                        If not provided, returns self for chaining.

        Returns:
            The minimum value in the column for this group.

        Example:
            >>> g.min('price')  # Min price in group
        """
        if column_name is None:
            return self

        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        valid_values = [v for v in values if v is not None]
        return min(valid_values) if valid_values else None

    def sum(self, column_name: str = None) -> Any:
        """
        Get the sum of values in a column for this group.

        Args:
            column_name: Name of the column to sum.
                        If not provided, returns self for chaining.

        Returns:
            The sum of values in the column for this group.

        Example:
            >>> g.sum('quantity')  # Total quantity in group
        """
        if column_name is None:
            return self

        if isinstance(self._group_data, list):
            values = [
                row.get(column_name, 0) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        valid_values = [v for v in values if v is not None]
        return sum(valid_values) if valid_values else 0

    def avg(self, column_name: str = None) -> Any:
        """
        Get the average value in a column for this group.

        Args:
            column_name: Name of the column to average.
                        If not provided, returns self for chaining.

        Returns:
            The average value in the column for this group.

        Example:
            >>> g.avg('price')  # Average price in group
        """
        if column_name is None:
            return self

        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        valid_values = [v for v in values if v is not None]
        if valid_values:
            return sum(valid_values) / len(valid_values)
        return None

    def all(self, predicate: Callable) -> bool:
        """
        Check if predicate holds for ALL rows in this group.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
                      E.g., lambda r: r.amount > 0

        Returns:
            True if predicate is True for all rows, False otherwise.

        Example:
            >>> g.all(lambda r: r.price > 0)  # All prices positive?
        """
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if not predicate(row_proxy):
                return False
        return True

    def any(self, predicate: Callable) -> bool:
        """
        Check if predicate holds for ANY row in this group.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
                      E.g., lambda r: r.status == "error"

        Returns:
            True if predicate is True for at least one row, False otherwise.

        Example:
            >>> g.any(lambda r: r.is_vip == True)  # Any VIP customers?
        """
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                return True
        return False

    def none(self, predicate: Callable) -> bool:
        """
        Check if predicate holds for NO rows in this group.

        Equivalent to `not any(predicate)`.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
                      E.g., lambda r: r.is_deleted == True

        Returns:
            True if predicate is False for all rows, False otherwise.

        Example:
            >>> g.none(lambda r: r.is_deleted == True)  # No deleted rows?
        """
        return not self.any(predicate)

    def _iterate_rows(self):
        """Iterate over rows in the group, yielding dict-like objects."""
        if isinstance(self._group_data, list):
            for row in self._group_data:
                yield row if isinstance(row, dict) else row.to_dict()
        else:
            # pandas DataFrame
            for _, row in self._group_data.iterrows():
                yield row.to_dict()
