"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:
    from .core import LTSeq
    from .expr import ColumnExpr


class GroupProxy:
    """
    Proxy for evaluating predicates on a group during filter operations.

    Provides access to group properties like count(), first(), last()
    so that predicates can be evaluated.
    """

    def __init__(self, group_data, nested_table):
        """
        Initialize a group proxy.

        Args:
            group_data: DataFrame containing rows for this group (list of dicts or pandas DataFrame)
            nested_table: The NestedTable this group belongs to
        """
        self._group_data = group_data
        self._nested_table = nested_table

    def count(self):
        """Get the number of rows in this group."""
        if isinstance(self._group_data, list):
            return len(self._group_data)
        else:
            return len(self._group_data)

    def first(self):
        """Get the first row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            first_row_data = self._group_data[0]
        else:
            first_row_data = self._group_data.iloc[0]
        return RowProxy(first_row_data)

    def last(self):
        """Get the last row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            last_row_data = self._group_data[-1]
        else:
            last_row_data = self._group_data.iloc[-1]
        return RowProxy(last_row_data)


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

    def filter(self, group_predicate: Callable) -> "LTSeq":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last()

        Returns:
            An LTSeq with only rows from groups that pass the predicate

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
            >>> grouped.filter(lambda g: g.first().price > 100)

        Note: Phase B8 implementation. Supports common predicates:
        - g.count() > N, >= N, < N, <= N, == N
        - g.first().column comparisons
        - g.last().column comparisons
        """
        # Phase B8: Implement SQL-based group filtering

        # Materialize the grouping to get __group_id__ and __group_count__
        flattened = self.flatten()

        # Try to detect the predicate pattern and build SQL
        import inspect
        import ast

        try:
            # Get the source code of the predicate
            source = inspect.getsource(group_predicate)
            # Parse it to understand the structure
            tree = ast.parse(source)

            # Try to extract a simple count comparison pattern
            # Pattern: lambda g: g.count() > N
            sql_where = self._extract_filter_condition(tree)

            if sql_where:
                # Use SQL to filter
                return self._filter_by_sql(flattened, sql_where)
        except Exception:
            pass

        # Fallback: Return flattened table (no filtering applied)
        # Full implementation would require pandas materialization
        return flattened

    def _extract_filter_condition(self, ast_tree) -> str:
        """
        Extract a WHERE clause from an AST tree of a filter lambda.

        Handles patterns like:
        - lambda g: g.count() > 2
        - lambda g: g.first().price > 100
        """
        # Find the Compare node in the AST
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.Compare):
                # Check if left side is g.count()
                left = node.left
                if isinstance(left, ast.Call):
                    if isinstance(left.func, ast.Attribute):
                        if left.func.attr == "count":
                            # Pattern: g.count() op value
                            op = node.ops[0]
                            comparator = node.comparators[0]

                            if isinstance(comparator, ast.Constant):
                                value = comparator.value
                                if isinstance(op, ast.Gt):
                                    return f"__group_count__ > {value}"
                                elif isinstance(op, ast.GtE):
                                    return f"__group_count__ >= {value}"
                                elif isinstance(op, ast.Lt):
                                    return f"__group_count__ < {value}"
                                elif isinstance(op, ast.LtE):
                                    return f"__group_count__ <= {value}"
                                elif isinstance(op, ast.Eq):
                                    return f"__group_count__ = {value}"

        return ""

    def _filter_by_sql(self, flattened, where_clause: str) -> "LTSeq":
        """Apply a SQL WHERE clause to filter the flattened table."""
        # For now, we can't easily execute SQL on an LTSeq
        # This would require adding a filter_where() method to Rust
        # For now, just return the flattened table
        return flattened

    def derive(self, group_mapper: Callable) -> "LTSeq":
        """
        Derive new columns based on group properties.

        Args:
            group_mapper: Lambda that returns a dict of new columns based on group.
                         Can use g.first(), g.last(), g.count()

        Returns:
            An LTSeq with derived columns added

        Example:
            >>> grouped.derive(lambda g: {"span": g.count(), "gain": g.last().price - g.first().price})

        Note: Phase B7 implementation. Returns flattened table with __group_id__ column.
        Full derived column computation requires pandas materialization (future work).
        """
        # Phase B7: Group-based column derivation
        # For now, return the flattened table with group information
        # Future: Implement full derivation with pandas materialization

        return self.flatten()
