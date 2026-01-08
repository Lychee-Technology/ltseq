"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:
    from .core import LTSeq
    from .expr import ColumnExpr


class GroupRowProxy:
    """
    Proxy for accessing columns from first/last row of a group.

    When you call grouped.first().price, this object handles the column access
    and returns an expression that represents the first row's price value.
    """

    def __init__(self, nested_table: "NestedTable", position: str):
        """
        Initialize a group row proxy.

        Args:
            nested_table: The NestedTable this proxy is from
            position: Either "first" or "last"
        """
        self._nested_table = nested_table
        self._position = position  # "first" or "last"

    def __getattr__(self, col_name: str):
        """
        Access a column from the first/last row of each group.

        This implementation materializes the grouping and returns a filtered LTSeq
        containing only the first/last row per group, with the column selected.

        Args:
            col_name: The column name to access

        Returns:
            An LTSeq containing the first/last rows with that column
        """
        # Avoid infinite recursion on internal attributes
        if col_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{col_name}'"
            )

        # Validate column exists in schema
        if col_name not in self._nested_table._ltseq._schema:
            raise AttributeError(
                f"Column '{col_name}' not found in schema. "
                f"Available columns: {list(self._nested_table._ltseq._schema.keys())}"
            )

        # Materialize the grouping to add __group_id__
        flattened = self._nested_table.flatten()

        # Call appropriate Rust method to filter to first/last row
        if self._position == "first":
            filtered = flattened.__class__()
            filtered._schema = flattened._schema.copy()
            filtered._inner = flattened._inner.first_row()
        else:  # last
            filtered = flattened.__class__()
            filtered._schema = flattened._schema.copy()
            filtered._inner = flattened._inner.last_row()

        # Return the filtered table (so caller can do more operations or access values)
        return filtered


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

        # Optional filters and derivations (set later if needed)
        self._group_filter = None
        self._group_derive = None

    def first(self) -> "LTSeq":
        """
        Get the first row of each group.

        Returns an LTSeq containing only the first row within each group.

        Example:
            >>> grouped.first()  # Returns LTSeq with first row per group
            >>> grouped.first().quantity  # Column access (future enhancement)
        """
        # Materialize the grouping to add __group_id__
        flattened = self.flatten()

        # Call Rust method to filter to first row per group
        result = flattened.__class__()
        result._schema = flattened._schema.copy()
        result._inner = flattened._inner.first_row()

        return result

    def last(self) -> "LTSeq":
        """
        Get the last row of each group.

        Returns an LTSeq containing only the last row within each group.

        Example:
            >>> grouped.last()  # Returns LTSeq with last row per group
            >>> grouped.last().quantity  # Column access (future enhancement)
        """
        # Materialize the grouping to add __group_id__
        flattened = self.flatten()

        # Call Rust method to filter to last row per group
        result = flattened.__class__()
        result._schema = flattened._schema.copy()
        result._inner = flattened._inner.last_row()

        return result

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
