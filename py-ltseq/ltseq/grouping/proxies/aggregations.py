"""Aggregation methods mixin for GroupProxy."""

import statistics
from collections import Counter
from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from .row_proxy import RowProxy


class GroupAggregationMixin:
    """Mixin providing aggregation methods for GroupProxy."""

    # These will be provided by the base class
    _group_data: Any

    def _get_column_values(self, column_name: str) -> List[Any]:
        """Get all values for a column from group data."""
        if isinstance(self._group_data, list):
            return [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            return self._group_data[column_name].values.tolist()

    def _get_valid_values(self, column_name: str) -> List[Any]:
        """Get non-None values for a column from group data."""
        values = self._get_column_values(column_name)
        return [v for v in values if v is not None]

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

        valid_values = self._get_valid_values(column_name)
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

        valid_values = self._get_valid_values(column_name)
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

        valid_values = self._get_valid_values(column_name)
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

        valid_values = self._get_valid_values(column_name)
        if valid_values:
            return sum(valid_values) / len(valid_values)
        return None

    def median(self, column_name: str = None) -> Any:
        """
        Get the median value in a column for this group.

        Args:
            column_name: Name of the column to compute median for.
                        If not provided, returns self for chaining.

        Returns:
            The median value in the column for this group.

        Example:
            >>> g.median('price')  # Median price in group
        """
        if column_name is None:
            return self

        valid_values = self._get_valid_values(column_name)
        if valid_values:
            return statistics.median(valid_values)
        return None

    def percentile(self, column_name: str, p: float) -> Any:
        """
        Get the p-th percentile value in a column for this group.

        Args:
            column_name: Name of the column to compute percentile for.
            p: Percentile value between 0 and 1 (e.g., 0.95 for 95th percentile).

        Returns:
            The p-th percentile value in the column for this group.

        Example:
            >>> g.percentile('price', 0.95)  # 95th percentile price in group
        """
        valid_values = sorted(self._get_valid_values(column_name))
        if not valid_values:
            return None

        # Use linear interpolation for percentile
        n = len(valid_values)
        idx = p * (n - 1)
        lower = int(idx)
        upper = min(lower + 1, n - 1)
        weight = idx - lower
        return valid_values[lower] * (1 - weight) + valid_values[upper] * weight

    def variance(self, column_name: str = None) -> Any:
        """
        Get the sample variance of values in a column for this group.

        Args:
            column_name: Name of the column to compute variance for.
                        If not provided, returns self for chaining.

        Returns:
            The sample variance of values in the column for this group.

        Example:
            >>> g.variance('price')  # Variance of prices in group
        """
        if column_name is None:
            return self

        valid_values = self._get_valid_values(column_name)
        if len(valid_values) >= 2:
            return statistics.variance(valid_values)
        return None

    # Alias for variance
    var = variance

    def std(self, column_name: str = None) -> Any:
        """
        Get the sample standard deviation of values in a column for this group.

        Args:
            column_name: Name of the column to compute standard deviation for.
                        If not provided, returns self for chaining.

        Returns:
            The sample standard deviation of values in the column for this group.

        Example:
            >>> g.std('price')  # Std dev of prices in group
        """
        if column_name is None:
            return self

        valid_values = self._get_valid_values(column_name)
        if len(valid_values) >= 2:
            return statistics.stdev(valid_values)
        return None

    # Alias for std
    stddev = std

    def mode(self, column_name: str = None) -> Any:
        """
        Get the mode (most frequent value) in a column for this group.

        Args:
            column_name: Name of the column to compute mode for.
                        If not provided, returns self for chaining.

        Returns:
            The most frequent value in the column for this group.
            If there are multiple modes, returns one of them.

        Example:
            >>> g.mode('category')  # Most common category in group
        """
        if column_name is None:
            return self

        valid_values = self._get_valid_values(column_name)
        if valid_values:
            try:
                return statistics.mode(valid_values)
            except statistics.StatisticsError:
                # No unique mode - return first most common
                counter = Counter(valid_values)
                return counter.most_common(1)[0][0]
        return None

    def top_k(self, column_name: str, k: int) -> List[Any]:
        """
        Get the top K values from a column in this group.

        Returns values sorted in descending order.

        Args:
            column_name: Name of the column to get top values from.
            k: Number of top values to return.

        Returns:
            List of top K values sorted descending.

        Example:
            >>> g.top_k('price', 3)  # Top 3 prices in group
        """
        valid_values = self._get_valid_values(column_name)
        valid_values.sort(reverse=True)
        return valid_values[:k]
