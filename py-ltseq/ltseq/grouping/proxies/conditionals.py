"""Conditional methods mixin for GroupProxy."""

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    pass


class GroupConditionalMixin:
    """Mixin providing conditional predicate methods for GroupProxy."""

    # These will be provided by the base class
    _group_data: Any

    def _iterate_rows(self):
        """Iterate over rows in the group, yielding dict-like objects."""
        if isinstance(self._group_data, list):
            for row in self._group_data:
                yield row if isinstance(row, dict) else row.to_dict()
        else:
            # pandas DataFrame
            for _, row in self._group_data.iterrows():
                yield row.to_dict()

    def _get_row_value(self, row: Any, column_name: str) -> Any:
        """Get a column value from a row."""
        if isinstance(row, dict):
            return row.get(column_name)
        else:
            return row[column_name]

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
        from .row_proxy import RowProxy

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
        from .row_proxy import RowProxy

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

    def count_if(self, predicate: Callable) -> int:
        """
        Count rows where predicate is True.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
                      E.g., lambda r: r.price > 100

        Returns:
            Number of rows where predicate is True.

        Example:
            >>> g.count_if(lambda r: r.price > 100)  # Count expensive items
        """
        from .row_proxy import RowProxy

        count = 0
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                count += 1
        return count

    def sum_if(self, predicate: Callable, column_name: str) -> Any:
        """
        Sum column values where predicate is True.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
            column_name: Name of the column to sum.

        Returns:
            Sum of column values where predicate is True.

        Example:
            >>> g.sum_if(lambda r: r.category == "A", "amount")  # Sum amounts for category A
        """
        from .row_proxy import RowProxy

        total = 0
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                val = self._get_row_value(row, column_name)
                if val is not None:
                    total += val
        return total

    def avg_if(self, predicate: Callable, column_name: str) -> Any:
        """
        Average column values where predicate is True.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
            column_name: Name of the column to average.

        Returns:
            Average of column values where predicate is True, or None if no matching rows.

        Example:
            >>> g.avg_if(lambda r: r.status == "active", "score")  # Avg score for active
        """
        from .row_proxy import RowProxy

        total = 0
        count = 0
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                val = self._get_row_value(row, column_name)
                if val is not None:
                    total += val
                    count += 1
        return total / count if count > 0 else None

    def min_if(self, predicate: Callable, column_name: str) -> Any:
        """
        Minimum column value where predicate is True.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
            column_name: Name of the column to find minimum.

        Returns:
            Minimum column value where predicate is True, or None if no matching rows.

        Example:
            >>> g.min_if(lambda r: r.is_valid, "price")  # Min price for valid items
        """
        from .row_proxy import RowProxy

        values = []
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                val = self._get_row_value(row, column_name)
                if val is not None:
                    values.append(val)
        return min(values) if values else None

    def max_if(self, predicate: Callable, column_name: str) -> Any:
        """
        Maximum column value where predicate is True.

        Args:
            predicate: Lambda function that takes a row proxy and returns boolean.
            column_name: Name of the column to find maximum.

        Returns:
            Maximum column value where predicate is True, or None if no matching rows.

        Example:
            >>> g.max_if(lambda r: r.is_valid, "price")  # Max price for valid items
        """
        from .row_proxy import RowProxy

        values = []
        for row in self._iterate_rows():
            row_proxy = RowProxy(row)
            if predicate(row_proxy):
                val = self._get_row_value(row, column_name)
                if val is not None:
                    values.append(val)
        return max(values) if values else None
