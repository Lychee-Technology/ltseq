"""LinkedTable class for pointer-based join operations."""

from typing import TYPE_CHECKING, Callable, Optional, Union

from .expr import _lambda_to_expr
from .helpers import _extract_join_keys

if TYPE_CHECKING:
    from .core import LTSeq


class LinkedTable:
    """
    Represents a table with pointer-based foreign key references.

    Created by LTSeq.link(), this wrapper maintains pointers to rows in a target
    table without materializing an expensive join. Accessing linked columns is
    translated to index-based lookups (take operations) during execution.

    MVP Phase 8: Basic pointer table support with schema awareness.
    """

    def __init__(
        self,
        source_table: "LTSeq",
        target_table: "LTSeq",
        join_fn: Callable,
        alias: str,
        join_type: str = "inner",
    ):
        """
        Initialize a LinkedTable from source and target tables.

        Phase 8I: Enhanced to support multiple join types.

        Args:
            source_table: The primary table (e.g., orders)
            target_table: The table being linked (e.g., products)
            join_fn: Lambda that specifies the join condition
            alias: Alias used for the linked reference (e.g., "prod")
            join_type: Type of join ("inner", "left", "right", "full"). Default: "inner"
        """
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        self._join_type = join_type
        self._schema = source_table._schema.copy()
        self._materialized: Optional["LTSeq"] = None  # Phase 8B: Lazy materialization

        # Add linked column metadata to schema with prefix
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type

    def _materialize(self) -> "LTSeq":
        """
        Phase 8B: Materialize the join operation.

        This method performs the actual join only when data access is needed.
        The result is cached to avoid re-joining on subsequent accesses.

        Returns:
            LTSeq instance with materialized joined data

        Raises:
            TypeError: If join condition cannot be parsed
            ValueError: If join columns don't exist
        """
        # Return cached result if already materialized
        if self._materialized is not None:
            return self._materialized

        # Extract join keys using Phase 8A function (now with Phase 8I join_type support)
        left_key_expr, right_key_expr, join_type = _extract_join_keys(
            self._join_fn,
            self._source._schema,
            self._target._schema,
            self._join_type,  # Phase 8I: Pass the join type parameter
        )

        # Call Rust join() method - it handles all schema conflicts and column renaming
        joined_inner = self._source._inner.join(
            self._target._inner,
            left_key_expr,
            right_key_expr,
            join_type,
            self._alias,
        )

        # Create new LTSeq wrapping the joined result
        from .core import LTSeq

        result = LTSeq()
        result._inner = joined_inner
        result._schema = self._schema.copy()

        # Cache the result
        self._materialized = result
        return result

    def show(self, n: int = 10) -> None:
        """Display linked table with materialized joined data."""
        materialized = self._materialize()
        materialized.show(n)

    def filter(self, predicate: Callable) -> Union["LinkedTable", "LTSeq"]:
        """
        Filter rows with support for both source and linked columns.

        Phase 8F: Enhanced to materialize join if predicate references linked columns.

        If predicate uses only source columns, returns a LinkedTable with filtered source.
        If predicate uses linked columns, materializes the join and returns filtered LTSeq.

        Args:
            predicate: Lambda taking a row, returning boolean Expr
                      Can reference source columns (id, quantity) OR linked columns (prod.name, prod.price)

        Returns:
            LinkedTable if filtering on source columns only
            LTSeq if filtering on linked columns (join materialized)

        Raises:
            AttributeError: If predicate references non-existent columns
        """
        # Try to parse with source schema only
        try:
            _lambda_to_expr(predicate, self._source._schema)
            # Predicate only uses source columns - fast path
            filtered_source = self._source.filter(predicate)
            return LinkedTable(
                filtered_source, self._target, self._join_fn, self._alias
            )
        except AttributeError:
            # Predicate references columns not in source
            # These must be linked columns - materialize and filter
            materialized = self._materialize()
            return materialized.filter(predicate)

    def select(self, *cols) -> "LTSeq":
        """
        Select columns from linked table.

        Phase 11: Enhanced to transparently materialize join if selecting linked columns.
        Detects whether selected columns include linked columns (with alias prefix like 'prod_*')
        and materializes the join automatically if needed.

        Args:
            *cols: Column names to select (e.g., "id", "quantity", "prod_name", "prod_price")
                   Can mix source columns and linked columns in a single call

        Returns:
            LTSeq with selected columns (materialized if linked columns were selected)

        Example:
            >>> linked = orders.link(products, on=lambda o,p: o.product_id==p.product_id, as_="prod")
            >>> result = linked.select("id", "quantity")  # ✓ Works: source columns only
            >>> result = linked.select("prod_name")  # ✓ Works: linked column (materializes transparently)
            >>> result = linked.select("id", "prod_price")  # ✓ Works: mixed source and linked
        """
        # Check if any selected columns are linked columns
        # Linked columns have the format: "{alias}_{column_name}"
        has_linked_columns = any(
            col.startswith(f"{self._alias}_") for col in cols if isinstance(col, str)
        )

        # If a lambda is passed, we need to materialize to access linked columns
        has_lambda = any(callable(col) for col in cols)

        if has_linked_columns or has_lambda:
            # Materialize the join and select from the materialized table
            materialized = self._materialize()
            return materialized.select(*cols)
        else:
            # Select only from source columns (don't materialize)
            return self._source.select(*cols)

    def aggregate(self, *agg_specs, **named_aggs) -> "LTSeq":
        """
        Aggregate data from linked table.

        Phase 13: Perform aggregation on linked table data.
        Automatically materializes the join, making all columns available
        (both source and linked columns) for use with group_ordered.

        For true aggregation, use the pattern:
            linked.group_ordered(lambda r: grouping_key).derive(aggregation_dict)

        Args:
            *agg_specs: Not currently used (reserved for future aggregate API)
            **named_aggs: Not currently used (reserved for future aggregate API)

        Returns:
            LTSeq (materialized result with all columns available)

        Example:
            >>> linked = orders.link(products, on=..., as_="prod")
            >>> # Simple grouping and aggregation
            >>> grouped = linked.group_ordered(lambda r: r.prod_id)
            >>> result = grouped.derive(lambda g: {
            ...     "total_quantity": g.quantity.sum(),
            ...     "avg_price": g.prod_price.mean()
            ... })
            >>> result.flatten().show()
        """
        # Materialize the join and return it
        # This makes all columns (source + linked) available for further operations
        materialized = self._materialize()

        if agg_specs or named_aggs:
            # Support for explicit aggregation specifications is reserved for Phase 13+
            # For now, just materialize (which enables group_ordered + derive workflow)
            pass

        return materialized

    def derive(self, mapper: Callable) -> "LinkedTable":
        """Derive columns (delegates to source for MVP)."""
        derived_source = self._source.derive(mapper)
        return LinkedTable(derived_source, self._target, self._join_fn, self._alias)

    def sort(self, *key_exprs) -> "LinkedTable":
        """
        Sort linked table rows.

        Phase 11: Added sort support for LinkedTable.

        Args:
            *key_exprs: Column names or lambda expressions to sort by

        Returns:
            A new LinkedTable with sorted source data
        """
        sorted_source = self._source.sort(*key_exprs)
        return LinkedTable(sorted_source, self._target, self._join_fn, self._alias)

    def slice(self, start: int, end: int) -> "LinkedTable":
        """Slice rows."""
        sliced_source = self._source.slice(start, end)
        return LinkedTable(sliced_source, self._target, self._join_fn, self._alias)

    def distinct(self, key_fn: Callable = None) -> "LinkedTable":
        """Get distinct rows."""
        if key_fn is None:
            distinct_source = self._source.distinct()
        else:
            distinct_source = self._source.distinct(key_fn)
        return LinkedTable(distinct_source, self._target, self._join_fn, self._alias)

    def link(
        self, target_table: "LTSeq", on: Callable, as_: str, join_type: str = "inner"
    ) -> "LinkedTable":
        """
        Link this linked table to another table.

        Allows chaining multiple links while preserving previous schemas.

        Phase 8I: Enhanced to support join_type parameter.

        Args:
            target_table: The table to link to
            on: Lambda with two parameters specifying the join condition
            as_: Alias for the linked table reference
            join_type: Type of join ("inner", "left", "right", "full"). Default: "inner"

        Returns:
            A new LinkedTable with chained links
        """
        # Create new LinkedTable that chains from the source
        result = LinkedTable(self._source, target_table, on, as_, join_type)

        # Merge with existing schema from this LinkedTable
        for key, value in self._schema.items():
            if key not in result._schema:
                result._schema[key] = value

        return result
