"""Aggregation operations for LTSeq: agg, cum_sum, group_ordered, group_sorted."""

from typing import Callable, Optional, Union

from .expr import SchemaProxy
from .transforms import _collect_key_exprs


class AggregationMixin:
    """Mixin class providing aggregation operations for LTSeq."""

    def cum_sum(self, *cols: Union[str, Callable]) -> "LTSeq":
        """
        Add cumulative sum columns for specified columns.

        Calculates running sums across ordered rows. Requires data to be sorted.

        Args:
            *cols: Column names (str) or lambda expressions

        Returns:
            New LTSeq with cumulative sum columns (suffixed with '_cumsum')

        Example:
            >>> t.sort("date").cum_sum("revenue", "units")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not cols:
            raise ValueError("cum_sum() requires at least one column argument")

        cum_exprs = _collect_key_exprs(cols, self._schema, self._capture_expr)

        result = LTSeq()
        result._schema = self._schema.copy()

        for i, col_expr in enumerate(cols):
            if isinstance(col_expr, str):
                result._schema[f"{col_expr}_cumsum"] = result._schema.get(
                    col_expr, "float64"
                )
            else:
                result._schema[f"cum_sum_{i}"] = "float64"

        result._inner = self._inner.cum_sum(cum_exprs)
        result._sort_keys = self._sort_keys
        return result

    def group_ordered(self, grouping_fn: Callable) -> "NestedTable":
        """
        Group consecutive identical values based on a grouping function.

        Groups only consecutive rows with identical values in the grouping column.

        Args:
            grouping_fn: Lambda that extracts the grouping key.
                        E.g., lambda r: r.trend_flag

        Returns:
            NestedTable supporting group-level operations

        Example:
            >>> groups = t.sort("date").group_ordered(lambda r: r.is_up)
        """
        from .grouping import NestedTable

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(grouping_fn):
            raise TypeError(
                f"grouping_fn must be callable, got {type(grouping_fn).__name__}"
            )

        return NestedTable(self, grouping_fn, is_sorted=False)

    # Alias: group_consecutive is a more descriptive name for group_ordered
    group_consecutive = group_ordered

    def group_sorted(self, key: Callable) -> "NestedTable":
        """
        Group by key assuming data is globally sorted.

        Efficient one-pass grouping without hashing. Requires data to be sorted
        by the grouping key.

        Args:
            key: Lambda that extracts the grouping key.
                E.g., lambda r: r.user_id

        Returns:
            NestedTable supporting group-level operations

        Example:
            >>> groups = t.sort("user_id").group_sorted(lambda r: r.user_id)
        """
        from .grouping import NestedTable

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(key):
            raise TypeError(f"key must be callable, got {type(key).__name__}")

        return NestedTable(self, key, is_sorted=True)

    def agg(self, by: Optional[Callable] = None, **aggregations) -> "LTSeq":
        """
        Grouped aggregation with one row per group.

        Args:
            by: Grouping key lambda (None for full-table aggregation)
            **aggregations: Aggregation expressions as keyword arguments

        Returns:
            Aggregated LTSeq

        Example:
            >>> t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not aggregations:
            raise ValueError("agg() requires at least one aggregation expression")

        grouping_expr = None
        if by is not None:
            if not callable(by):
                raise TypeError(f"'by' must be callable, got {type(by).__name__}")
            grouping_expr = self._capture_expr(by)

        agg_exprs = {}
        for name, fn in aggregations.items():
            if not callable(fn):
                raise TypeError(
                    f"Aggregation '{name}' must be callable, got {type(fn).__name__}"
                )
            agg_exprs[name] = self._capture_expr(fn)

        result_inner = self._inner.agg(grouping_expr, agg_exprs)

        result = LTSeq()
        result._inner = result_inner

        result._schema = {}
        if grouping_expr and grouping_expr.get("type") == "Column":
            col_name = grouping_expr.get("name", "group_key")
            result._schema[col_name] = self._schema.get(col_name, "Unknown")
        for name in aggregations.keys():
            result._schema[name] = "Unknown"

        result._sort_keys = None
        return result
