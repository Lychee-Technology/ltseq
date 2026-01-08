"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:
    from .core import LTSeq


class NestedTable:
    """
    Represents a table grouped by consecutive identical values.

    Created by LTSeq.group_ordered(), this wrapper provides group-level operations
    like first(), last(), count() while maintaining the underlying row data.
    """

    def __init__(self, ltseq_instance: "LTSeq", grouping_lambda: Callable):
        """
        Initialize a NestedTable from an LTSeq and grouping function.

        Args:
            ltseq_instance: The LTSeq table to group
            grouping_lambda: Lambda that returns the grouping key for each row
        """
        self._ltseq = ltseq_instance
        self._grouping_lambda = grouping_lambda
        self._schema = ltseq_instance._schema.copy()

        # Add internal group columns to schema
        self._schema["__group_id__"] = "int64"
        self._schema["__group_count__"] = "int64"

    def first(self) -> "ColumnExpr":
        """
        Get the first row of each group.

        Returns a special ColumnExpr that represents the first row within each group.

        Example:
            >>> grouped.first().price  # First price in each group
        """
        from .expr import CallExpr

        # Create a special marker that will be handled by Rust
        return CallExpr("__first__", (), {}, on=None)

    def last(self) -> "ColumnExpr":
        """
        Get the last row of each group.

        Returns a special ColumnExpr that represents the last row within each group.

        Example:
            >>> grouped.last().price  # Last price in each group
        """
        from .expr import CallExpr

        return CallExpr("__last__", (), {}, on=None)

    def count(self) -> "ColumnExpr":
        """
        Get the count of rows in each group.

        Returns a ColumnExpr representing the row count for each group.

        Example:
            >>> grouped.count()  # Number of rows in each group
        """
        from .expr import CallExpr

        return CallExpr("__count__", (), {}, on=None)

    def flatten(self) -> "LTSeq":
        """
        Return the underlying LTSeq with __group_id__ column added.

        The __group_id__ column identifies consecutive groups with identical
        grouping values. For example, if grouping by is_up, all consecutive
        rows with is_up=1 get the same __group_id__.

        Phase B2: Materializes the lazy grouping by computing and adding the
        __group_id__ column via the consecutive identical grouping algorithm.

        Returns:
            A new LTSeq with __group_id__ column for each row

        Example:
            >>> grouped = t.group_ordered(lambda r: r.is_up)
            >>> flattened = grouped.flatten()  # Now has __group_id__ column
        """
        # Capture the grouping expression
        expr_dict = self._ltseq._capture_expr(self._grouping_lambda)

        # Create a new LTSeq with group_id computed
        result = self._ltseq.__class__()
        result._schema = self._schema.copy()  # Already includes __group_id__

        # Call Rust to compute __group_id__ column
        result._inner = self._ltseq._inner.group_id(expr_dict)

        return result

    def filter(self, group_predicate: Callable) -> "NestedTable":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last()

        Returns:
            A new NestedTable with filtered groups

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
        """
        try:
            # Create a new NestedTable that combines both predicates
            original_grouping = self._grouping_lambda

            def combined_filter(r):
                # First check the grouping condition
                grouping_key = original_grouping(r)
                # Then check the group predicate (simplified for now)
                return grouping_key

            result = NestedTable(self._ltseq, combined_filter)
            result._group_filter = group_predicate
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to filter groups: {e}")

    def derive(self, group_mapper: Callable) -> "NestedTable":
        """
        Derive new columns based on group properties.

        Args:
            group_mapper: Lambda that returns a dict of new columns based on group.
                         Can use g.first(), g.last(), g.count()

        Returns:
            A new NestedTable with derived columns

        Example:
            >>> grouped.derive(lambda g: {"span": g.count(), "gain": g.last().price - g.first().price})
        """
        try:
            result = NestedTable(self._ltseq, self._grouping_lambda)
            result._group_derive = group_mapper
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to derive group columns: {e}")
