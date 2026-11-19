"""Aggregation operations for LTSeq: agg, cum_sum, group_ordered, group_sorted, group_by."""

from typing import Any, Callable, TYPE_CHECKING

from ._draws import kept_names, renamed, stage_draws, unstage
from ._typing import LTSeqLike

if TYPE_CHECKING:
    from .core import LTSeq
    from .grouping import NestedTable

from .transforms import _collect_key_exprs


def _capture_agg_expr(table: "LTSeqLike", fn: Callable) -> dict[str, Any]:
    """Capture one aggregate lambda of ``group_by().agg()`` or ``LTSeq.agg()``
    against the group proxy, which adds ``g.count()`` to the columns (#314)."""
    from .expr.proxy import GroupAggProxy
    from .expr.transforms import _lambda_to_expr

    if not table._schema:
        raise ValueError(
            "Schema not initialized. Call read_csv() first to populate the schema."
        )
    return _lambda_to_expr(fn, table._schema, GroupAggProxy)


def _agg_staged(inner: Any, grouping_expr: Any, agg_exprs: dict[str, Any]) -> Any:
    """``inner.agg(grouping_expr, agg_exprs)``, with each ``rand()`` drawn once
    per row in a column beneath the aggregate, as in ``filter`` (R1-F2 of
    #413's review). A computed key keeps the name it has without staging."""
    names = list(agg_exprs)
    exprs = list(agg_exprs.values())
    keyed = grouping_expr is not None
    staged, rewritten, _ = stage_draws(inner, ([grouping_expr] if keyed else []) + exprs)
    if keyed:
        renames = kept_names(inner, staged, [grouping_expr], rewritten[:1])
        key = rewritten[0]
        rewritten = rewritten[1:]
    else:
        renames, key = {}, None
    # The output holds only the key and the aggregates, never a draw.
    return renamed(staged.agg(key, dict(zip(names, rewritten))), renames)


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
                           E.g., total=lambda g: g.sales.sum()

        Each lambda receives the group ``g``. Aggregates are methods of a
        column (``g.sales.sum()``, ``g.id.count()``, ...); ``g.col.first()``
        and ``g.col.last()`` give the value of the group's first or last row
        in table order, NULL included. ``g.count()`` counts the group's rows.
        A column named ``count`` keeps its name, so ``g.count`` is then that
        column (``g.count.sum()``) while ``g.count()`` still counts rows.
        ``g.first().col``, the form of ``group_ordered(...).agg()``, is not
        available here: ``g.first`` reads as a column. Linked-table
        columns keep their access: with a ``first_v`` or ``count_v`` column
        (``link(..., as_="first")``), ``g.first.v`` and ``g.count.v`` read it.

        Returns:
            Aggregated LTSeq with one row per group

        Raises:
            SortRequiredError: If an aggregate uses ``first`` or ``last`` and
                the table declares no order. Call ``.sort(...)`` or
                ``.assume_sorted(...)`` first; ``.min()`` / ``.max()`` need
                no order.

        Example:
            >>> t.group_by("region").agg(total=lambda g: g.sales.sum())
            >>> t.group_by(lambda r: r.region).agg(total=lambda g: g.sales.sum(), n=lambda g: g.count())
            >>> t.sort("date").group_by("region").agg(opened=lambda g: g.date.first())
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
            agg_exprs[name] = _capture_agg_expr(self._table, fn)

        result_inner = _agg_staged(self._table._inner, grouping_expr, agg_exprs)

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

        # One value per row for each rand() (R1-F2 of #413's review); the
        # sums are named by position, so staging leaves their names.
        staged, cum_exprs, draws = stage_draws(self._inner, cum_exprs)
        result_inner = unstage(staged.cum_sum(cum_exprs), draws)
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

        Groups consecutive rows with equal keys, as ``group_ordered`` does
        (the same plan), after checking that the key leads the declared sort
        order, so that each key value forms one group. Raises
        ``SortRequiredError`` when the table has no declared order or the key
        does not lead it.

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
            **aggregations: Aggregation expressions as keyword arguments.
                Each lambda receives the group ``g``, as in
                ``group_by().agg()``: ``g.count()`` counts rows, and
                ``g.col.first()`` / ``g.col.last()`` need a declared order
                (``SortRequiredError`` otherwise).

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
            agg_exprs[name] = _capture_agg_expr(self, fn)

        result_inner = _agg_staged(self._inner, grouping_expr, agg_exprs)

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
