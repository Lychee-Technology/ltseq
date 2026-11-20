"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, cast

from .._draws import _columns, rename_draws, stage_draws, unstage
from ..expr.transforms import _invoke_group_lambda
from .proxies import FilterGroupProxy, FilterExpr

if TYPE_CHECKING:
    from ..core import LTSeq
    from ..expr import CallExpr, ColumnExpr


class NestedTable:
    """
    Represents a table grouped by consecutive identical values.

    Created by LTSeq.group_ordered() or LTSeq.group_sorted(), this wrapper provides
    group-level operations like first(), last(), count() while maintaining the
    underlying row data.

    The difference between group_ordered and group_sorted is semantic:
    - group_ordered: groups consecutive identical values in the declared row order
    - group_sorted: assumes data is globally sorted by key (consecutive = all same key)

    Both use the same underlying algorithm (consecutive grouping), but group_sorted
    documents the expectation that data is pre-sorted.
    """

    def __init__(
        self,
        ltseq_instance: "LTSeq",
        grouping_lambda: Callable,
        is_sorted: bool = False,
    ):
        """
        Initialize a NestedTable from an LTSeq and grouping function.

        Args:
            ltseq_instance: The LTSeq table to group
            grouping_lambda: Lambda that returns the grouping key for each row
            is_sorted: If True, indicates the data is globally sorted by the key
                      (from group_sorted). If False, groups only consecutive
                      identical values (from group_ordered).
        """
        self._ltseq = ltseq_instance
        self._grouping_lambda = grouping_lambda
        self._is_sorted = is_sorted

        # Optional filters and derivations
        self._group_filter = None
        self._group_derive = None

        # Set by filter() when the key holds rand() (R1-F4 of #413's
        # review): the captured key, reading the draws staged as columns of
        # ``ltseq_instance``, and those columns, which no result shows.
        self._key_expr: "dict[str, Any] | None" = None
        self._hidden: list[str] = []

    def _with_hidden(self, key_expr: "dict[str, Any]", hidden: list[str]) -> "NestedTable":
        """This grouping keyed by ``key_expr``, over a table holding the
        columns ``hidden`` that its results drop."""
        self._key_expr = key_expr
        self._hidden = hidden
        return self

    def _visible_schema(self) -> dict[str, str]:
        """The schema of the grouped table without its hidden columns."""
        return {
            name: dtype
            for name, dtype in self._ltseq._schema.items()
            if name not in self._hidden
        }

    def _staged_key(
        self, taken: "set[str] | frozenset[str]" = frozenset()
    ) -> "tuple[Any, dict[str, Any], list[str]]":
        """The grouped table with each ``rand()`` of the key staged as a
        column, the key reading those columns, and every column the results
        drop (#298). The staged columns take none of the names in ``taken``,
        the columns an expression evaluated over them reads, which then stay
        missing columns (R3-F1 of #413's review)."""
        if self._key_expr is not None:
            staged, (key_expr,), hidden = rename_draws(
                self._ltseq._inner, [self._key_expr], self._hidden, taken
            )
            return staged, key_expr, hidden
        expr_dict = self._ltseq._capture_expr(self._grouping_lambda)
        staged, (expr_dict,), draws = stage_draws(self._ltseq._inner, [expr_dict], taken)
        return staged, expr_dict, draws

    def __len__(self) -> int:
        """Return the number of rows in the grouped table."""
        return len(self._ltseq)

    def to_pandas(self) -> Any:
        """Convert the grouped table to a pandas DataFrame."""
        if self._hidden:
            from ..core import LTSeq

            return LTSeq._from_inner(unstage(self._ltseq._inner, self._hidden)).to_pandas()
        return self._ltseq.to_pandas()

    def first(self) -> "LTSeq":
        """
        Get the first row of each group.

        Returns an LTSeq containing only the first row within each group,
        with the groups in sequence order. Its sort_keys is None.
        The returned LTSeq has a fast path for .count() that avoids
        materializing per-row metadata arrays.

        Example:
            >>> grouped.first()  # Returns LTSeq with first row per group
            >>> grouped.first().count()  # Fast path: just counts groups
        """
        return cast("LTSeq", _LazyFirstLTSeq(self))

    def last(self) -> "LTSeq":
        """
        Get the last row of each group.

        Returns an LTSeq containing only the last row within each group,
        with the groups in sequence order. Its sort_keys is None.

        Example:
            >>> grouped.last()  # Returns LTSeq with last row per group
        """
        flattened, names = self._flatten()
        # Construct through __init__ so a subclass keeps the state it sets
        # there. The schema cache stays unset so it is read from the
        # result's own plan, which projects the grouping columns away;
        # copying flattened's would list __group_id__, __group_count__ and
        # __rn__ (#315).
        result = type(flattened)()
        result._inner = flattened._inner.last_row(names)
        result._schema_cache = None
        return result

    def count(self) -> "CallExpr":
        """
        Get the count of rows in each group.

        Returns a ColumnExpr representing the row count for each group.

        Example:
            >>> grouped.count()  # Number of rows in each group
        """
        from ..expr import CallExpr

        return CallExpr("__count__", (), {}, on=None)

    def flatten(self) -> "LTSeq":
        """
        Return the underlying LTSeq with __group_id__ column added.

        The __group_id__ column identifies consecutive groups with identical
        grouping values. ``__group_count__`` (the group's row count) and
        ``__rn__`` (the row's number in its group) follow it. When the table
        already has a column of one of these names, that column keeps its
        name and values, and the grouping column takes the first free name
        of its kind instead (``__group_id_1__``, ``__group_id_2__``, ...).

        Returns:
            A new LTSeq with __group_id__ column for each row

        Example:
            >>> grouped = t.group_ordered(lambda r: r.is_up)
            >>> flattened = grouped.flatten()  # Now has __group_id__ column
        """
        return self._flatten()[0]

    def _flatten(
        self, avoid: "list[str] | None" = None
    ) -> "tuple[LTSeq, tuple[str, str, str]]":
        """flatten(), and the names its grouping columns took: the group id,
        the group's row count and the row number in the group (#346). None
        of them is a column of the grouped table or a name in ``avoid``."""
        # The key is compared with the previous row's key; with rand() in it
        # each row's one draw is compared with the previous row's (#298).
        staged, expr_dict, hidden = self._staged_key()
        flat, names = staged.group_id(expr_dict, avoid or [])

        # The schema is read from the plan: the grouping columns' names are
        # the ones group_id chose, and a user column keeps its own type.
        result = self._ltseq.__class__()
        result._inner = unstage(flat, hidden)
        result._schema_cache = None

        return result, names

    def filter(self, group_predicate: Callable) -> "NestedTable":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last(), g.max('col'), etc.

        Returns:
            A NestedTable with only rows from groups that pass the predicate

        Raises:
            TypeError: If the lambda, a helper it names or a lambda nested in
                it compares a value with None using 'is' / 'is not' (#300).
                Group lambdas are not rewritten; use g.first().col.is_null()
                / .is_not_null() (or == None / != None) instead.

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
            >>> grouped.filter(lambda g: g.first().price > 100)
            >>> grouped.filter(lambda g: g.all(lambda r: r.amount > 0))
        """
        result = _invoke_group_lambda(group_predicate, FilterGroupProxy(self._visible_schema()))

        if not isinstance(result, FilterExpr):
            raise ValueError(
                "group_ordered().filter() predicate must return a boolean expression. "
                "Supported patterns include:\n"
                "  - g.count() > N, g.count() < N, g.count() >= N, etc.\n"
                "  - g.first().column op value (e.g., g.first().price > 100)\n"
                "  - g.last().column op value\n"
                "  - g.max('column') op value, g.min(), g.sum(), g.avg()\n"
                "  - g.first().column.is_null() / .is_not_null() "
                "(or == None / != None; 'is None' is not supported here)\n"
                "  - g.all(lambda r: r.col op val), g.any(...), g.none(...)\n"
                "  - Combinations with & (AND) and | (OR)\n"
                "If you need a predicate that cannot be expressed this way, consider "
                "using .flatten() and then .filter() on the flattened result."
            )

        # Serialized group dialect → native staged filter in Rust (issue #91):
        # derive hidden __group_filter__ window column → filter → project
        # the original columns. No SQL strings, plan stays lazy.
        from ..core import LTSeq

        # The rows a group filter keeps keep the draws their key read (R1-F4
        # of #413's review): grouping them again draws no new key, so the
        # groups that passed stay whole. A rand() in the predicate is drawn
        # once per row beneath the filter, and dropped after it.
        # The predicate reads the columns the user sees: a column it names
        # like a staged draw of the key stays missing (R3-F1).
        # The grouping columns take none of the names the predicate reads
        # either (#346): a column it names that the table lacks stays missing
        # rather than reading one of them.
        predicate = result.serialize()
        read = _columns(predicate)
        staged, key_expr, hidden = self._staged_key(read)
        flat, names = staged.group_id(key_expr, sorted(read))
        flat, (predicate,), draws = stage_draws(flat, [predicate])
        inner = unstage(flat.filter_group_window(predicate, names), draws)
        nested = NestedTable(
            LTSeq._from_inner(inner), self._grouping_lambda, is_sorted=self._is_sorted
        )
        if hidden:
            nested._with_hidden(key_expr, hidden)
        return nested

    def derive(self, group_mapper: Callable) -> "LTSeq":
        """
        Derive new columns based on group properties.

        Each group property (first row, last row, count, etc.) is broadcasted
        to all rows in that group.

        Args:
            group_mapper: Lambda taking a group proxy (g) and returning a dict
                         of new columns.

        Returns:
            LTSeq with original rows plus new derived columns

        Raises:
            TypeError: If the lambda, a helper it names or a lambda nested in
                it compares a value with None using 'is' / 'is not', or
                compares a group value with None using '==' / '!=' (#300).
                Group values here have no null test (no .is_null() /
                .is_not_null(), unlike in filter()): emit the group value as
                a column and test it with a row lambda afterwards
                (``from ltseq import if_else``)::

                    (grouped.derive(lambda g: {"first_b": g.first().b, "n": g.count()})
                        .derive(lambda r: {"n_if_b": if_else(r.first_b.is_not_null(), r.n, None)}))

        Example:
            >>> grouped.derive(lambda g: {"span": g.count()})
            >>> grouped.derive(lambda g: {
            ...     "start": g.first().date,
            ...     "end": g.last().date,
            ... })
        """
        derive_exprs = self._capture_derive_via_proxy(group_mapper)
        # The grouping columns take none of the output names and none of the
        # columns the expressions read (#346): an output may have any name,
        # and a column the table lacks stays missing rather than reading one
        # of them.
        flattened, names = self._flatten(
            avoid=[*derive_exprs, *sorted(_columns(list(derive_exprs.values())))]
        )
        from ..core import LTSeq

        return LTSeq._from_inner(flattened._inner.derive_group_window(derive_exprs, names))

    def agg(self, group_mapper: Callable) -> "LTSeq":
        """
        Collapse each group to a single summary row.

        The counterpart of derive(): derive broadcasts group values to every
        row (SQL window semantics), agg returns one row per group (SQL
        GROUP BY semantics). Groups appear in their original sequence order.

        Args:
            group_mapper: Lambda taking a group proxy (g) and returning a dict
                         of output columns. Supports g.count(), g.first().col,
                         g.last().col, and g.sum/avg/mean/min/max/median/std/
                         var/percentile('col').

        Returns:
            LTSeq with one row per group, containing only the mapped columns

        Raises:
            TypeError: If the lambda, a helper it names or a lambda nested in
                it compares a value with None using 'is' / 'is not', or
                compares a group value with None using '==' / '!=' (#300).
                Group values here have no null test (no .is_null() /
                .is_not_null(), unlike in filter()): emit the group value as
                a column and test it with a row lambda afterwards, or with a
                row-level derive and if_else as in derive()::

                    (grouped.agg(lambda g: {"first_b": g.first().b, "n": g.count()})
                        .filter(lambda r: r.first_b.is_not_null()))

        Example:
            >>> spans = t.group_ordered(lambda r: r.is_up).agg(lambda g: {
            ...     "start": g.first().date,
            ...     "end": g.last().date,
            ...     "n": g.count(),
            ... })
        """
        from .proxies.derive_proxy import DeriveGroupProxy
        from .expr import GroupExpr
        from ..core import LTSeq

        result = _invoke_group_lambda(group_mapper, DeriveGroupProxy())

        if not isinstance(result, dict) or not result:
            raise ValueError("agg lambda must return a non-empty dict of group expressions")

        for col_name, expr in result.items():
            if not isinstance(col_name, str):
                raise ValueError(f"Column name must be a string, got {type(col_name)}")
            if not isinstance(expr, GroupExpr):
                raise ValueError(
                    f"Unsupported expression type for column '{col_name}': {type(expr)}. "
                    f"Expected a plain group aggregate (g.count(), g.first().col, "
                    f"g.sum('col'), ...); arithmetic combinations are not supported "
                    f"in agg() yet — use derive() + distinct or compute after agg()"
                )

        # The grouping columns take names no input column and no output has
        # (#346): the group id is the group-by key beside the outputs, and
        # the sort key below. Nor do they take a name the expressions read:
        # a column the table lacks stays missing rather than reading one of
        # them.
        read = _columns([expr.serialize() for expr in result.values()])
        flattened, (group_id, _, rn) = self._flatten(avoid=[*result, *sorted(read)])
        agg_exprs = {
            col_name: self._group_expr_to_agg_call(expr, group_id, rn)
            for col_name, expr in result.items()
        }
        inner = flattened._inner.agg({"type": "Column", "name": group_id}, agg_exprs)
        out = LTSeq._from_inner(inner)
        # Aggregate output order is not guaranteed: restore sequence order via
        # the group id, then project it away.
        return out.sort(group_id).select(*result.keys())

    @staticmethod
    def _group_expr_to_agg_call(expr: "Any", group_id: str, rn: str) -> dict:
        """Translate a group-dialect expression into the row-dialect aggregate
        Call dict that the native agg engine understands, over the grouping
        columns ``group_id`` and ``rn`` that flatten() added."""
        from .expr import GroupAggExpr, GroupCountExpr, GroupRowColumnExpr
        from ..expr.types import CallExpr, ColumnExpr

        if isinstance(expr, GroupCountExpr):
            return CallExpr("count", (), {}, on=ColumnExpr(group_id)).serialize()
        if isinstance(expr, GroupRowColumnExpr):
            # first/last row value, ordered by the in-group row number
            return CallExpr(
                expr._row_type,
                (ColumnExpr(rn),),
                {},
                on=ColumnExpr(expr._column),
            ).serialize()
        if isinstance(expr, GroupAggExpr):
            args = () if expr._arg is None else (expr._arg,)
            return CallExpr(expr._func, args, {}, on=ColumnExpr(expr._column)).serialize()
        raise ValueError(
            f"Unsupported group expression in agg(): {type(expr)}. "
            f"Use g.count(), g.first().col, g.last().col, or g.<agg>('col')"
        )

    def _capture_derive_via_proxy(self, group_mapper: Callable) -> dict[str, str]:
        """
        Capture derive expressions using proxy pattern.

        This approach works in all contexts (pytest, REPL, exec) because it
        doesn't rely on source code inspection.

        Args:
            group_mapper: Lambda taking a group proxy and returning dict of expressions

        Returns:
            Dict mapping column name to SQL window function expression

        Raises:
            ValueError: If the lambda doesn't return a dict of GroupExpr objects
        """
        from .proxies.derive_proxy import DeriveGroupProxy
        from .expr import GroupExpr

        result = _invoke_group_lambda(group_mapper, DeriveGroupProxy())

        if not isinstance(result, dict):
            raise ValueError("Derive lambda must return a dict")

        derive_exprs = {}
        for col_name, expr in result.items():
            if not isinstance(col_name, str):
                raise ValueError(f"Column name must be a string, got {type(col_name)}")

            if isinstance(expr, GroupExpr):
                derive_exprs[col_name] = expr.serialize()
            else:
                raise ValueError(
                    f"Unsupported expression type for column '{col_name}': {type(expr)}. "
                    f"Expected GroupExpr (from g.count(), g.first().col, etc.)"
                )

        return derive_exprs

class _LazyFirstLTSeq:
    """
    Lazy wrapper returned by NestedTable.first().

    Defers materialization until needed. Intercepts count()/len() to use the
    fast group_ordered_count() Rust path that avoids building per-row metadata
    arrays (saves ~0.71s on 100M rows).

    For all other operations, materializes eagerly via flatten().first_row()
    and delegates to the resulting LTSeq.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    def __init__(self, nested: "NestedTable"):
        # Intentionally do NOT call LTSeq.__init__ — we are lazy.
        self._nested = nested
        self._materialized = None
        # LTSeq._schema is a lazy property backed by _inner, which this lazy
        # wrapper doesn't have yet — pre-seed the cache from the source table
        # so schema reads work before materialization.
        self._schema_cache = nested._visible_schema()
        self._name = None

    @property
    def _sort_keys(self):
        """Sort keys without forcing materialization.

        LTSeq._sort_keys reads self._inner, which this lazy wrapper lacks
        until materialized; going through __getattr__ would materialize as
        a side effect. Before materialization, answer from the source table
        (first_row preserves its sort specs, so the value is identical).
        """
        source = self._materialized if self._materialized is not None else self._nested._ltseq
        keys = source._inner.get_sort_keys()
        return keys if keys else None

    def _materialize(self):
        """Eagerly materialize the first-row-per-group LTSeq."""
        if self._materialized is None:
            flattened, names = self._nested._flatten()
            from ..core import LTSeq
            inner = flattened._inner.first_row(names)
            result = LTSeq._from_inner(inner)
            self._materialized = result
        return self._materialized

    # -- Fast path: count / len --

    def count(self) -> int:
        """Fast path: count groups without materializing per-row arrays."""
        # As flatten() does (#298).
        staged, expr_dict, _ = self._nested._staged_key()
        try:
            return staged.group_ordered_count(expr_dict)
        except Exception:
            # Fallback: materialize and count
            return self._materialize().count()

    def __len__(self) -> int:
        return self.count()

    # -- Delegate everything else to materialized LTSeq --

    def __getattr__(self, name):
        """Delegate attribute access to the materialized LTSeq."""
        return getattr(self._materialize(), name)
