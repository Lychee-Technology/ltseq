"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable

from .proxies import FilterGroupProxy, FilterExpr
from .sql_parsing import group_expr_to_sql

if TYPE_CHECKING:
    from ..core import LTSeq
    from ..expr import ColumnExpr


class NestedTable:
    """
    Represents a table grouped by consecutive identical values.

    Created by LTSeq.group_ordered() or LTSeq.group_sorted(), this wrapper provides
    group-level operations like first(), last(), count() while maintaining the
    underlying row data.

    The difference between group_ordered and group_sorted is semantic:
    - group_ordered: groups consecutive identical values (no assumption about sorting)
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
        result = _LazyFirstLTSeq(self)
        return result

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

    def count(self) -> "ColumnExpr":
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
        proxy = FilterGroupProxy()
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

        where_clause = result.to_sql()
        if not where_clause:
            raise ValueError(
                "group_ordered().filter() could not convert the predicate to SQL."
            )

        return self._filter_via_sql(where_clause)

    def _filter_via_sql(self, where_clause: str) -> "NestedTable":
        """Filter using SQL WHERE clause with window functions.

        Window functions cannot appear in SQL WHERE clauses. We extract all
        window function expressions from the where_clause, add them as derived
        columns via derive_window_sql, then filter on the simplified condition
        using only column references.
        """
        import re

        _FILTER_COL = "__filter_cond__"

        # Find all window function expressions in the where_clause.
        # Strategy: find each "OVER (PARTITION BY __group_id__...)" pattern,
        # then scan backwards past any whitespace and matching parens to find
        # the start of the window function expression (e.g. "COUNT(*)",
        # "MIN(CASE WHEN ... END)", etc.)
        window_exprs = []
        simplified = where_clause

        pattern = r'OVER\s*\(\s*PARTITION\s+BY\s+__group_id__'
        pos = 0
        while True:
            m = re.search(pattern, simplified[pos:])
            if not m:
                break
            over_start = pos + m.start()

            # Find the closing paren of OVER(...)
            # First find opening paren after OVER
            rest = simplified[over_start + len("OVER"):]
            paren_m = re.match(r'\s*\(', rest)
            if not paren_m:
                break
            paren_open = over_start + len("OVER") + paren_m.end() - 1

            # Find matching closing paren
            depth = 1
            i = paren_open + 1
            while i < len(simplified) and depth > 0:
                if simplified[i] == '(':
                    depth += 1
                elif simplified[i] == ')':
                    depth -= 1
                i += 1
            window_end = i  # exclusive (character after closing paren)

            # Scan backwards from OVER keyword to find expr start.
            # Skip whitespace immediately before OVER, then walk backwards
            # through any parenthesized expression and function name.
            j = over_start - 1
            # Skip whitespace before OVER
            while j >= 0 and simplified[j] in (' ', '\t', '\n'):
                j -= 1
            # Now walk backwards through matching parens and identifier chars
            paren_depth = 0
            while j >= 0:
                ch = simplified[j]
                if ch == ')':
                    paren_depth += 1
                elif ch == '(':
                    if paren_depth == 0:
                        break
                    paren_depth -= 1
                elif paren_depth == 0 and not (ch.isalnum() or ch in ('_', '.', '*', '"', "'")):
                    break
                j -= 1
            expr_start = j + 1

            window_expr = simplified[expr_start:window_end]
            col_name = f"{_FILTER_COL}_{len(window_exprs)}"
            window_exprs.append((col_name, window_expr))

            # Replace the window expression with the column reference
            simplified = simplified[:expr_start] + f'"{col_name}"' + simplified[window_end:]
            # Continue scanning after the replacement
            pos = expr_start + len(f'"{col_name}"')

        if not window_exprs:
            raise ValueError(
                f"No window functions found in filter condition: {where_clause}"
            )

        # Build the derive expressions dict
        filter_exprs = {col_name: expr for col_name, expr in window_exprs}

        flattened = self.flatten()

        # Step 1: Add filter conditions as derived columns
        derived_inner = flattened._inner.derive_window_sql(filter_exprs)

        derived_ltseq = flattened.__class__()
        derived_ltseq._inner = derived_inner
        derived_schema = flattened._schema.copy()
        for col_name in filter_exprs:
            derived_schema[col_name] = "int64"
        derived_ltseq._schema = derived_schema

        # Step 2: Filter on the simplified condition
        filter_condition = simplified
        filtered_inner = derived_ltseq._inner.filter_where(filter_condition)

        # Step 3: Remove filter and internal columns via select
        original_cols = list(self._ltseq._schema.keys())
        result_ltseq = derived_ltseq.__class__()
        result_ltseq._inner = filtered_inner
        result_ltseq._schema = derived_schema

        result_ltseq = result_ltseq.select(*original_cols)

        result_nested = NestedTable(
            result_ltseq, self._grouping_lambda, is_sorted=self._is_sorted
        )
        return result_nested

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
        return self._derive_via_sql(flattened, derive_exprs)

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
                derive_exprs[col_name] = group_expr_to_sql(expr.serialize())
            else:
                raise ValueError(
                    f"Unsupported expression type for column '{col_name}': {type(expr)}. "
                    f"Expected GroupExpr (from g.count(), g.first().col, etc.)"
                )

        return derive_exprs

    def _derive_via_sql(
        self, flattened: "LTSeq", derive_exprs: dict[str, str]
    ) -> "LTSeq":
        """Apply derived column expressions via SQL SELECT with window functions."""
        result = flattened.__class__()

        result._schema = {}
        for col_name, col_type in self._ltseq._schema.items():
            if col_name not in ("__group_id__", "__rn__", "__group_count__"):
                result._schema[col_name] = col_type
        for col_name in derive_exprs.keys():
            result._schema[col_name] = "Unknown"

        result._inner = flattened._inner.derive_window_sql(derive_exprs)

        return result


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
        # Set LTSeq-expected attributes to avoid AttributeError before
        # __getattr__ kicks in (these are checked by some LTSeq methods).
        self._schema = nested._ltseq._schema.copy()
        self._sort_keys = nested._ltseq._sort_keys
        self._name = None

    def _materialize(self):
        """Eagerly materialize the first-row-per-group LTSeq."""
        if self._materialized is None:
            flattened = self._nested.flatten()
            from ..core import LTSeq
            inner = flattened._inner.first_row()
            result = LTSeq._from_inner(inner)
            result._schema = flattened._schema.copy()
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
