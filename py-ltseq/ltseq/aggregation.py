"""Aggregation operations for LTSeq: agg, cum_sum, group_ordered, group_sorted, group_by."""

from typing import Any, Callable, TYPE_CHECKING

from ._typing import LTSeqLike

if TYPE_CHECKING:
    from .core import LTSeq
    from .grouping import NestedTable

from .transforms import _collect_key_exprs


class GroupBy:
    """
    Intermediate object returned by LTSeq.group_by().

    Supports Polars/pandas-style chaining: t.group_by("col").agg(total=lambda r: r.sales.sum())
    """

    def __init__(self, table: "LTSeq", key: str | Callable):
        self._table = table
        self._key = key

    def agg(self, **aggregations: Callable) -> "LTSeq":
        """
        Aggregate each group into a single row.

        Args:
            **aggregations: Named aggregation expressions.
                           E.g., total=lambda r: r.sales.sum()

        Returns:
            Aggregated LTSeq with one row per group

        Example:
            >>> t.group_by("region").agg(total=lambda r: r.sales.sum())
            >>> t.group_by(lambda r: r.region).agg(total=lambda r: r.sales.sum(), n=lambda r: r.id.count())
        """
        from .core import LTSeq

        if not aggregations:
            raise ValueError("agg() requires at least one aggregation expression")

        # Build grouping expression
        if isinstance(self._key, str):
            # String column name → Column expr
            if self._key not in self._table._schema:
                raise AttributeError(
                    f"Column '{self._key}' not found. "
                    f"Available columns: {list(self._table._schema.keys())}"
                )
            grouping_expr: dict[str, Any] = {"type": "Column", "name": self._key}
        else:
            grouping_expr = self._table._capture_expr(self._key)

        agg_exprs = {}
        for name, fn in aggregations.items():
            if not callable(fn):
                raise TypeError(
                    f"Aggregation '{name}' must be callable, got {type(fn).__name__}"
                )
            agg_exprs[name] = self._table._capture_expr(fn)

        result_inner = self._table._inner.agg(grouping_expr, agg_exprs)

        return LTSeq._from_inner(result_inner)


class AggregationMixin(LTSeqLike):
    """Mixin class providing aggregation operations for LTSeq."""

    def cum_sum(self, *cols: str | Callable) -> "LTSeq":
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

        # Same ordering invariant as window expressions in derive() (issue
        # #125): a running sum over an undeclared row order is nondeterministic
        # on multi-partition plans.
        if not self._sort_keys:
            from .transforms import _window_sort_required_error

            raise _window_sort_required_error()

        cum_exprs = _collect_key_exprs(cols, self._schema, self._capture_expr)

        result_inner = self._inner.cum_sum(cum_exprs)
        return LTSeq._from_inner(result_inner)

    def group_ordered(self, grouping_fn: Callable) -> "NestedTable":
        """
        Group consecutive identical values based on a grouping function.

        Groups only consecutive rows with identical values in the grouping
        column, in the table's DECLARED row order: call .sort(...) first, or
        .assume_sorted(...) if the data is already ordered (issue #125 —
        undeclared physical order is not stable on multi-partition plans).

        Args:
            grouping_fn: Lambda that extracts the grouping key.
                        E.g., lambda r: r.trend_flag

        Returns:
            NestedTable supporting group-level operations

        Example:
            >>> groups = t.sort("date").group_ordered(lambda r: r.is_up)
        """
        from .grouping import NestedTable
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(grouping_fn):
            raise TypeError(
                f"grouping_fn must be callable, got {type(grouping_fn).__name__}"
            )

        # group_ordered() groups CONSECUTIVE rows, so its result is defined by
        # row order. An undeclared physical order is not stable on
        # multi-partition plans (issue #125): require .sort() or, for data
        # already ordered on disk, .assume_sorted().
        if not self._sort_keys:
            from .exceptions import SortRequiredError
            from .transforms import _COMPUTED_SORT_HINT

            raise SortRequiredError(
                "group_ordered() groups consecutive rows and therefore "
                "requires a declared row order. Call .sort(...) first, or "
                ".assume_sorted(...) if the data is already ordered."
                + _COMPUTED_SORT_HINT
            )

        return NestedTable(self if isinstance(self, LTSeq) else LTSeq._from_inner(self._inner), grouping_fn, is_sorted=False)

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
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(key):
            raise TypeError(f"key must be callable, got {type(key).__name__}")

        # Capture first so invalid columns raise ColumnNotFound-style errors
        # before any sortedness complaint.
        key_expr = self._capture_expr(key)

        # Validate the sortedness the API name promises (issue #125): the
        # grouping key must lead the declared sort order, or interleaved keys
        # would be split into multiple consecutive runs instead of one group
        # per key. .assume_sorted(key) is the trust-the-caller escape hatch.
        if not self._sort_keys:
            from .exceptions import SortRequiredError

            raise SortRequiredError(
                "group_sorted() requires the table to be sorted by the "
                "grouping key. Call .sort(key) first, or .assume_sorted(key) "
                "if the data is already ordered."
            )
        if key_expr.get("type") == "Column":
            first_sort_col = self._sort_keys[0][0]
            if key_expr.get("name") != first_sort_col:
                from .exceptions import SortRequiredError

                raise SortRequiredError(
                    f"group_sorted() key '{key_expr.get('name')}' does not "
                    f"lead the declared sort order (sorted by "
                    f"'{first_sort_col}' first). Sort by the grouping key, "
                    "or use .assume_sorted() to declare the actual order."
                )
        else:
            # A computed grouping key cannot be checked against column-level
            # sort metadata, and letting it through unchecked silently splits
            # logical keys into multiple runs whenever the table is sorted by
            # something else (PR #126 review P1). Sorted-by-g also does not
            # prove sorted-by-f(g) for arbitrary f, so reject uniformly.
            from .exceptions import SortRequiredError

            raise SortRequiredError(
                "group_sorted() cannot verify a computed grouping key "
                "against the declared sort order. Derive the key as a "
                "column and sort by it first, e.g. "
                ".derive(k=...).sort('k').group_sorted(lambda r: r.k)."
            )

        return NestedTable(self if isinstance(self, LTSeq) else LTSeq._from_inner(self._inner), key, is_sorted=True)

    def agg(self, by: Callable | None = None, **aggregations: Callable) -> "LTSeq":
        """
        Grouped aggregation with one row per group.

        Args:
            by: Grouping key lambda (None for full-table aggregation)
            **aggregations: Aggregation expressions as keyword arguments

        Returns:
            Aggregated LTSeq

        Example:
            >>> t.agg(by=lambda r: r.region, total=lambda r: r.sales.sum())
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

        return LTSeq._from_inner(result_inner)

    def group_by(self, key: "str | Callable") -> "GroupBy":
        """
        Start a grouped aggregation using the Polars/pandas chain style.

        Returns a GroupBy intermediate object on which .agg() is called.

        Args:
            key: Column name (str) or lambda returning the grouping key.
                 E.g., "region" or lambda r: r.region

        Returns:
            GroupBy object

        Example:
            >>> t.group_by("region").agg(total=lambda r: r.sales.sum())
            >>> t.group_by(lambda r: r.region).agg(total=lambda r: r.sales.sum())
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(key, str) and not callable(key):
            raise TypeError(
                f"group_by() key must be a column name (str) or callable, "
                f"got {type(key).__name__}"
            )

        from .core import LTSeq

        table = self if isinstance(self, LTSeq) else LTSeq._from_inner(self._inner)
        return GroupBy(table, key)
