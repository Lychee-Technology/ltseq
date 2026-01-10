"""Join operations for LTSeq: join, join_merge, join_sorted, asof_join."""

from typing import Callable, List, Optional, Tuple, Union

from .expr import SchemaProxy


def _pandas_join_fallback(
    df1, df2, left_key: str, right_key: str, how: str, suffix: str
):
    """Execute join using pandas as fallback."""
    import pandas as pd

    pandas_how = "outer" if how == "full" else how
    common_cols = sorted(set(df1.columns) & set(df2.columns))

    if left_key == right_key and left_key in common_cols:
        merged = pd.merge(df1, df2, on=left_key, how=pandas_how, suffixes=("", suffix))
    else:
        merged = pd.merge(
            df1,
            df2,
            left_on=left_key,
            right_on=right_key,
            how=pandas_how,
            suffixes=("", suffix),
        )

    return merged


def _validate_join_inputs(self_table, other, how: str):
    """Validate join inputs and return error if invalid."""
    if not self_table._schema:
        raise ValueError(
            "Schema not initialized. Call read_csv() first to populate the schema."
        )

    if not isinstance(other, type(self_table)):
        raise TypeError(f"join() argument must be LTSeq, got {type(other).__name__}")

    if not other._schema:
        raise ValueError("Other table schema not initialized. Call read_csv() first.")

    valid_join_types = {"inner", "left", "right", "full"}
    if how not in valid_join_types:
        raise ValueError(
            f"Invalid join type '{how}'. Must be one of: {valid_join_types}"
        )


def _build_join_result_schema(left_schema, right_schema, suffix="_other"):
    """Build result schema for join operations.

    Rust joins always prefix ALL right columns with {suffix}_{col},
    so we must match that behavior here.
    """
    result_schema = left_schema.copy()
    for col_name, col_type in right_schema.items():
        # Always add suffix prefix to right columns (matching Rust behavior)
        result_schema[f"{suffix}_{col_name}"] = col_type
    return result_schema


class JoinMixin:
    """Mixin class providing join operations for LTSeq."""

    def join(self, other: "LTSeq", on: Callable, how: str = "inner") -> "LTSeq":
        """
        Standard SQL-style hash join between two tables.

        Args:
            other: Another LTSeq table to join with
            on: Lambda specifying join condition.
                E.g., lambda a, b: a.user_id == b.user_id
            how: Join type: "inner", "left", "right", "full"

        Returns:
            Joined LTSeq with columns from both tables

        Example:
            >>> result = users.join(orders, on=lambda u, o: u.id == o.user_id)
        """
        from .core import LTSeq

        _validate_join_inputs(self, other, how)

        # Validate join condition
        try:
            self_proxy = SchemaProxy(self._schema)
            other_proxy = SchemaProxy(other._schema)
            _ = on(self_proxy, other_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        from .helpers import _extract_join_keys

        left_key_expr, right_key_expr, jtype = _extract_join_keys(
            on, self._schema, other._schema, how
        )

        try:
            joined_inner = self._inner.join(
                other._inner, left_key_expr, right_key_expr, jtype, "_other"
            )
        except RuntimeError as e:
            import warnings

            warnings.warn(
                f"Rust join failed: {e}. Falling back to pandas implementation.",
                RuntimeWarning,
            )
            return self._join_pandas_fallback(other, on, how, left_key_expr)

        result = LTSeq()
        result._inner = joined_inner
        result._schema = _build_join_result_schema(
            self._schema, other._schema, "_other"
        )
        return result

    def _join_pandas_fallback(
        self, other, on: Callable, how: str, left_key_expr: dict
    ) -> "LTSeq":
        """Fallback join implementation using pandas."""
        from .core import LTSeq

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "join() fallback requires pandas. Install with: pip install pandas"
            )

        df1 = self.to_pandas()
        df2 = other.to_pandas()

        left_key = left_key_expr.get("name", "")
        right_key = left_key  # Simplified - assume same key name

        merged = _pandas_join_fallback(df1, df2, left_key, right_key, how, "_other")
        rows = merged.to_dict("records")

        result_schema = _build_join_result_schema(self._schema, other._schema, "_other")
        return LTSeq._from_rows(rows, result_schema)

    def join_merge(
        self,
        other: "LTSeq",
        on: Callable,
        join_type: str = "inner",
    ) -> "LTSeq":
        """
        Merge join for pre-sorted tables.

        Both tables should be sorted by the join key for optimal O(N+M) performance.

        Args:
            other: Another LTSeq table
            on: Lambda specifying join condition
            join_type: Join type: "inner", "left", "right", "full"

        Returns:
            Joined LTSeq

        Example:
            >>> result = t1.sort("id").join_merge(t2.sort("id"), on=lambda a, b: a.id == b.id)
        """
        from .core import LTSeq

        _validate_join_inputs(self, other, join_type)

        try:
            self_proxy = SchemaProxy(self._schema)
            other_proxy = SchemaProxy(other._schema)
            _ = on(self_proxy, other_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        from .helpers import _extract_join_keys

        left_key_expr, right_key_expr, jtype = _extract_join_keys(
            on, self._schema, other._schema, join_type
        )

        try:
            joined_inner = self._inner.join(
                other._inner, left_key_expr, right_key_expr, jtype, "_other"
            )
        except RuntimeError as e:
            import warnings

            warnings.warn(
                f"Rust join_merge failed: {e}. Falling back to pandas.",
                RuntimeWarning,
            )
            return self._join_pandas_fallback(other, on, join_type, left_key_expr)

        result = LTSeq()
        result._inner = joined_inner
        result._schema = _build_join_result_schema(
            self._schema, other._schema, "_other"
        )
        return result

    def join_sorted(self, other: "LTSeq", on: Callable, how: str = "inner") -> "LTSeq":
        """
        Merge join with sort order validation.

        Validates both tables are sorted by join keys before executing.

        Args:
            other: Another LTSeq table (must be sorted by join key)
            on: Lambda specifying join condition
            how: Join type: "inner", "left", "right", "full"

        Returns:
            Joined LTSeq

        Raises:
            ValueError: If tables not sorted by join keys

        Example:
            >>> t1_sorted = t1.sort("id")
            >>> t2_sorted = t2.sort("id")
            >>> result = t1_sorted.join_sorted(t2_sorted, on=lambda a, b: a.id == b.id)
        """
        from .core import LTSeq

        _validate_join_inputs(self, other, how)

        try:
            self_proxy = SchemaProxy(self._schema)
            other_proxy = SchemaProxy(other._schema)
            _ = on(self_proxy, other_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        from .helpers import _extract_join_keys

        left_key_expr, right_key_expr, jtype = _extract_join_keys(
            on, self._schema, other._schema, how
        )

        # Extract column names for validation
        left_key_col = left_key_expr.get("name") if left_key_expr else None
        right_key_col = right_key_expr.get("name") if right_key_expr else None

        # Get sort directions from tables
        left_desc = self._get_sort_direction(left_key_col) if left_key_col else None
        right_desc = other._get_sort_direction(right_key_col) if right_key_col else None

        # Validate sort order (using the actual direction from sort_keys)
        if left_key_col:
            expected_desc = left_desc if left_desc is not None else False
            if not self.is_sorted_by(left_key_col, desc=expected_desc):
                raise ValueError(
                    f"Left table is not sorted by '{left_key_col}'. "
                    f"Use .sort('{left_key_col}') first or use join_merge() instead."
                )

        if right_key_col:
            expected_desc = right_desc if right_desc is not None else False
            if not other.is_sorted_by(right_key_col, desc=expected_desc):
                raise ValueError(
                    f"Right table is not sorted by '{right_key_col}'. "
                    f"Use .sort('{right_key_col}') first or use join_merge() instead."
                )

        # Check sort directions match
        if left_key_col and right_key_col:
            if left_desc is not None and right_desc is not None:
                if left_desc != right_desc:
                    raise ValueError(
                        f"Sort directions don't match: left '{left_key_col}' is "
                        f"{'descending' if left_desc else 'ascending'}, "
                        f"right '{right_key_col}' is "
                        f"{'descending' if right_desc else 'ascending'}."
                    )

        # Execute merge join
        try:
            joined_inner = self._inner.join(
                other._inner, left_key_expr, right_key_expr, jtype, "_other"
            )
        except RuntimeError as e:
            import warnings

            warnings.warn(
                f"Rust join_sorted failed: {e}. Falling back to pandas.",
                RuntimeWarning,
            )
            return self._join_pandas_fallback(other, on, how, left_key_expr)

        result = LTSeq()
        result._inner = joined_inner
        result._schema = _build_join_result_schema(
            self._schema, other._schema, "_other"
        )
        return result

    def _get_sort_direction(self, col_name: str) -> Optional[bool]:
        """Get sort direction for a column, or None if not sorted by it."""
        if not self._sort_keys:
            return None
        for key, desc in self._sort_keys:
            if key == col_name:
                return desc
        return None

    def asof_join(
        self,
        other: "LTSeq",
        on: Callable,
        direction: str = "backward",
        is_sorted: bool = False,
    ) -> "LTSeq":
        """
        As-of join for time-series data.

        Finds nearest match in other table based on time/key column.

        Args:
            other: Another LTSeq table
            on: Lambda specifying join condition (e.g., lambda t, q: t.time >= q.time)
            direction: "backward" (<=), "forward" (>=), or "nearest"
            is_sorted: If True, skip sort verification (faster)

        Returns:
            Joined LTSeq

        Example:
            >>> result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"asof_join() argument must be LTSeq, got {type(other).__name__}"
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        valid_directions = {"backward", "forward", "nearest"}
        if direction not in valid_directions:
            raise ValueError(
                f"Invalid direction '{direction}'. Must be one of: {valid_directions}"
            )

        try:
            self_proxy = SchemaProxy(self._schema)
            other_proxy = SchemaProxy(other._schema)
            _ = on(self_proxy, other_proxy)
        except Exception as e:
            raise TypeError(f"Invalid asof join condition: {e}")

        from .helpers import _extract_asof_keys

        left_time_col, right_time_col, operator = _extract_asof_keys(
            on, self._schema, other._schema
        )

        left_table = self
        right_table = other

        if not is_sorted:
            left_needs_sort = not self.is_sorted_by(left_time_col)
            right_needs_sort = not other.is_sorted_by(right_time_col)

            if left_needs_sort:
                left_table = self.sort(left_time_col)
            if right_needs_sort:
                right_table = other.sort(right_time_col)

        try:
            joined_inner = left_table._inner.asof_join(
                right_table._inner,
                left_time_col,
                right_time_col,
                direction,
                "_other",
            )
        except RuntimeError as e:
            raise RuntimeError(f"Asof join failed: {e}")

        result = LTSeq()
        result._inner = joined_inner
        result._schema = _build_join_result_schema(
            self._schema, other._schema, "_other"
        )
        return result
