"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, cast

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
        self._schema = ltseq_instance._schema.copy()
        self._is_sorted = is_sorted

        # Add internal group columns to schema
        self._schema["__group_id__"] = "int64"
        self._schema["__group_count__"] = "int64"
        self._schema["__rn__"] = "int64"

        # Optional filters and derivations
        self._group_filter = None
        self._group_derive = None

    def __len__(self) -> int:
        """Return the number of rows in the grouped table."""
        return len(self._ltseq)

    def to_pandas(self) -> Any:
        """Convert the grouped table to a pandas DataFrame."""
        return self._ltseq.to_pandas()

    def first(self) -> "LTSeq":
        """
        Get the first row of each group.

        Returns an LTSeq containing only the first row within each group.
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

        Returns an LTSeq containing only the last row within each group.

        Example:
            >>> grouped.last()  # Returns LTSeq with last row per group
        """
        flattened = self.flatten()
        result = flattened.__class__()
        result._schema = flattened._schema.copy()
        result._inner = flattened._inner.last_row()
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
        grouping values.

        Returns:
            A new LTSeq with __group_id__ column for each row

        Example:
            >>> grouped = t.group_ordered(lambda r: r.is_up)
            >>> flattened = grouped.flatten()  # Now has __group_id__ column
        """
        expr_dict = self._ltseq._capture_expr(self._grouping_lambda)

        result = self._ltseq.__class__()
        result._schema = self._schema.copy()
        result._inner = self._ltseq._inner.group_id(expr_dict)

        return result

    def filter(self, group_predicate: Callable) -> "NestedTable":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last(), g.max('col'), etc.

        Returns:
            A NestedTable with only rows from groups that pass the predicate

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
            >>> grouped.filter(lambda g: g.first().price > 100)
            >>> grouped.filter(lambda g: g.all(lambda r: r.amount > 0))
        """
        proxy = FilterGroupProxy(self._ltseq._schema)
        result = group_predicate(proxy)

        if not isinstance(result, FilterExpr):
            raise ValueError(
                "group_ordered().filter() predicate must return a boolean expression. "
                "Supported patterns include:\n"
                "  - g.count() > N, g.count() < N, g.count() >= N, etc.\n"
                "  - g.first().column op value (e.g., g.first().price > 100)\n"
                "  - g.last().column op value\n"
                "  - g.max('column') op value, g.min(), g.sum(), g.avg()\n"
                "  - g.all(lambda r: r.col op val), g.any(...), g.none(...)\n"
                "  - Combinations with & (AND) and | (OR)\n"
                "If you need a predicate that cannot be expressed this way, consider "
                "using .flatten() and then .filter() on the flattened result."
            )

        # Serialized group dialect → native staged filter in Rust (issue #91):
        # derive hidden __group_filter__ window column → filter → project
        # the original columns. No SQL strings, plan stays lazy.
        from ..core import LTSeq

        inner = self.flatten()._inner.filter_group_window(result.serialize())
        return NestedTable(
            LTSeq._from_inner(inner), self._grouping_lambda, is_sorted=self._is_sorted
        )

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

        Example:
            >>> grouped.derive(lambda g: {"span": g.count()})
            >>> grouped.derive(lambda g: {
            ...     "start": g.first().date,
            ...     "end": g.last().date,
            ... })
        """
        flattened = self.flatten()
        derive_exprs = self._capture_derive_via_proxy(group_mapper)
        from ..core import LTSeq

        return LTSeq._from_inner(flattened._inner.derive_group_window(derive_exprs))

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

        proxy = DeriveGroupProxy()
        result = group_mapper(proxy)

        if not isinstance(result, dict) or not result:
            raise ValueError("agg lambda must return a non-empty dict of group expressions")

        # Internal columns injected by flatten()/agg for grouping and ordering.
        # A user output named after one of these would collide with the
        # group-by/sort key below, giving ambiguous results.
        reserved = ("__group_id__", "__rn__")

        agg_exprs = {}
        for col_name, expr in result.items():
            if not isinstance(col_name, str):
                raise ValueError(f"Column name must be a string, got {type(col_name)}")
            if col_name in reserved:
                raise ValueError(
                    f"Output column name '{col_name}' is reserved for internal use; "
                    f"choose a different name"
                )
            if not isinstance(expr, GroupExpr):
                raise ValueError(
                    f"Unsupported expression type for column '{col_name}': {type(expr)}. "
                    f"Expected a plain group aggregate (g.count(), g.first().col, "
                    f"g.sum('col'), ...); arithmetic combinations are not supported "
                    f"in agg() yet — use derive() + distinct or compute after agg()"
                )
            agg_exprs[col_name] = self._group_expr_to_agg_call(expr)

        flattened = self.flatten()
        inner = flattened._inner.agg(
            {"type": "Column", "name": "__group_id__"}, agg_exprs
        )
        out = LTSeq._from_inner(inner)
        # Aggregate output order is not guaranteed: restore sequence order via
        # the group id, then project it away.
        return out.sort("__group_id__").select(*result.keys())

    @staticmethod
    def _group_expr_to_agg_call(expr: "Any") -> dict:
        """Translate a group-dialect expression into the row-dialect aggregate
        Call dict that the native agg engine understands."""
        from .expr import GroupAggExpr, GroupCountExpr, GroupRowColumnExpr
        from ..expr.types import CallExpr, ColumnExpr

        if isinstance(expr, GroupCountExpr):
            return CallExpr("count", (), {}, on=ColumnExpr("__group_id__")).serialize()
        if isinstance(expr, GroupRowColumnExpr):
            # first/last row value, ordered by the in-group row number
            return CallExpr(
                expr._row_type,
                (ColumnExpr("__rn__"),),
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

        proxy = DeriveGroupProxy()
        result = group_mapper(proxy)

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
        self._schema_cache = dict(nested._ltseq._schema)
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
            flattened = self._nested.flatten()
            from ..core import LTSeq
            inner = flattened._inner.first_row()
            result = LTSeq._from_inner(inner)
            self._materialized = result
        return self._materialized

    # -- Fast path: count / len --

    def count(self) -> int:
        """Fast path: count groups without materializing per-row arrays."""
        nested = self._nested
        ltseq = nested._ltseq
        expr_dict = ltseq._capture_expr(nested._grouping_lambda)
        try:
            return ltseq._inner.group_ordered_count(expr_dict)
        except Exception:
            # Fallback: materialize and count
            return self._materialize().count()

    def __len__(self) -> int:
        return self.count()

    # -- Delegate everything else to materialized LTSeq --

    def __getattr__(self, name):
        """Delegate attribute access to the materialized LTSeq."""
        return getattr(self._materialize(), name)
