"""Advanced operations for LTSeq: align, stateful_scan, search_first, pivot, set operations."""

from typing import Any, Callable, Optional


class SetOpsMixin:
    """Mixin class providing set operations for LTSeq."""

    def union(self, other: "LTSeq") -> "LTSeq":
        """
        Vertical concatenation of two tables (UNION ALL).

        Args:
            other: Another LTSeq with same schema

        Returns:
            Combined LTSeq

        Example:
            >>> combined = t1.union(t2)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"union() argument must be LTSeq, got {type(other).__name__}"
            )

        if not other._schema:
            raise ValueError("Other table schema not initialized.")

        result_inner = self._inner.union(other._inner)

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def intersect(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
        """
        Intersection of two tables.

        Args:
            other: Another table
            on: Key selector (None means all columns)

        Returns:
            Intersection LTSeq

        Example:
            >>> common = t1.intersect(t2, on=lambda r: r.id)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"intersect() argument must be LTSeq, got {type(other).__name__}"
            )

        on_expr = None
        if on is not None:
            if not callable(on):
                raise TypeError(f"'on' must be callable, got {type(on).__name__}")
            on_expr = self._capture_expr(on)

        result_inner = self._inner.intersect(other._inner, on_expr)

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def except_(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
        """
        Set difference: rows in left but not in right (SQL EXCEPT).

        Args:
            other: Another table
            on: Key selector

        Returns:
            Difference LTSeq

        Example:
            >>> only_left = t1.except_(t2, on=lambda r: r.id)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"except_() argument must be LTSeq, got {type(other).__name__}"
            )

        on_expr = None
        if on is not None:
            if not callable(on):
                raise TypeError(f"'on' must be callable, got {type(on).__name__}")
            on_expr = self._capture_expr(on)

        result_inner = self._inner.diff(other._inner, on_expr)

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def diff(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
        """
        Deprecated: Use except_() instead.

        Set difference: rows in left but not in right.
        """
        import warnings

        warnings.warn(
            "diff() is deprecated for set difference. Use except_() instead. "
            "diff() will become a row-level difference operation in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.except_(other, on=on)

    def is_subset(self, other: "LTSeq", on: Optional[Callable] = None) -> bool:
        """
        Check if this table is a subset of another.

        Args:
            other: Another table
            on: Key selector

        Returns:
            True if this table is a subset

        Example:
            >>> flag = t_small.is_subset(t_big, on=lambda r: r.id)
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        from .core import LTSeq

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"is_subset() argument must be LTSeq, got {type(other).__name__}"
            )

        on_expr = None
        if on is not None:
            if not callable(on):
                raise TypeError(f"'on' must be callable, got {type(on).__name__}")
            on_expr = self._capture_expr(on)

        return self._inner.is_subset(other._inner, on_expr)


class AdvancedOpsMixin:
    """Mixin class providing advanced operations for LTSeq."""

    def align(self, ref_sequence: list, key: Callable) -> "LTSeq":
        """
        Align table rows to a reference sequence.

        Reorders rows to match ref_sequence order and inserts NULL rows
        for missing keys.

        Args:
            ref_sequence: List of key values defining output order
            key: Lambda extracting key column (e.g., lambda r: r.date)

        Returns:
            Aligned LTSeq

        Example:
            >>> aligned = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.date)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not ref_sequence:
            raise ValueError("ref_sequence cannot be empty")

        if not callable(key):
            raise TypeError(f"key must be callable, got {type(key).__name__}")

        key_expr = self._capture_expr(key)

        if key_expr.get("type") != "Column" or "name" not in key_expr:
            raise TypeError(
                "key must be a simple column reference (e.g., lambda r: r.date). "
                "Complex expressions are not supported."
            )

        key_col = key_expr["name"]

        if key_col not in self._schema:
            raise AttributeError(
                f"Column '{key_col}' not found in schema. "
                f"Available columns: {list(self._schema.keys())}"
            )

        try:
            result_inner = self._inner.align(ref_sequence, key_col)
        except RuntimeError as e:
            raise RuntimeError(f"align() failed: {e}")

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None

        return result

    def search_first(self, predicate: Callable) -> Optional["LTSeq"]:
        """
        Return the first matching row.

        Args:
            predicate: Lambda returning boolean expression

        Returns:
            Single-row LTSeq (empty if not found)

        Example:
            >>> first_big = t.sort("price").search_first(lambda r: r.price > 100)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(predicate):
            raise TypeError(
                f"predicate must be callable, got {type(predicate).__name__}"
            )

        expr_dict = self._capture_expr(predicate)

        try:
            result_inner = self._inner.search_first(expr_dict)

            result = LTSeq()
            result._inner = result_inner
            result._schema = self._schema.copy()
            return result
        except Exception:
            # Fallback to Python iteration
            df = self.to_pandas()
            for idx, row in df.iterrows():
                single_row_df = df.iloc[[idx]]
                rows = single_row_df.to_dict("records")
                try:
                    single_ltseq = LTSeq._from_rows(rows, self._schema)
                    result_filtered = single_ltseq.filter(predicate)
                    if len(result_filtered) > 0:
                        return result_filtered
                except Exception:
                    continue
            return LTSeq._from_rows([], self._schema)

    def pivot(
        self,
        index: str | list,
        columns: str,
        values: str,
        agg_fn: str = "sum",
    ) -> "LTSeq":
        """
        Pivot table from long to wide format.

        Args:
            index: Row index column(s)
            columns: Column to pivot
            values: Value field
            agg_fn: Aggregation function

        Returns:
            Pivoted LTSeq

        Example:
            >>> pivoted = t.pivot(index="date", columns="region", values="amount")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        index_cols = [index] if isinstance(index, str) else list(index)

        for col in index_cols:
            if col not in self._schema:
                raise AttributeError(
                    f"Index column '{col}' not found. Available: {list(self._schema.keys())}"
                )

        if columns not in self._schema:
            raise AttributeError(
                f"Columns column '{columns}' not found. Available: {list(self._schema.keys())}"
            )

        if values not in self._schema:
            raise AttributeError(
                f"Values column '{values}' not found. Available: {list(self._schema.keys())}"
            )

        result_inner = self._inner.pivot(index_cols, columns, values, agg_fn)

        result = LTSeq()
        result._inner = result_inner

        result_schema = {}
        for col in index_cols:
            result_schema[col] = self._schema[col]
        result_schema["_pivot_columns"] = "Unknown"
        result._schema = result_schema
        result._sort_keys = None

        return result

    def stateful_scan(
        self,
        func: Callable,
        init: Any,
        output_col: str = "scan_result",
    ) -> "LTSeq":
        """
        Stateful scan with user-defined state transition function.

        Iterates through rows maintaining state. The accumulated state
        at each row is stored in a new column.

        Args:
            func: State transition function (state, row) -> new_state
            init: Initial state value
            output_col: Name for output column (default: "scan_result")

        Returns:
            New LTSeq with state column added

        Example:
            >>> result = t.sort("date").stateful_scan(
            ...     func=lambda s, r: s * (1 + r["rate"]),
            ...     init=1.0,
            ...     output_col="cumulative_return"
            ... )
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not callable(func):
            raise TypeError(f"func must be callable, got {type(func).__name__}")

        if output_col in self._schema:
            raise ValueError(
                f"Output column '{output_col}' already exists in schema. "
                f"Use a different name or remove the existing column first."
            )

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "stateful_scan() requires pandas. Install with: pip install pandas"
            )

        df = self.to_pandas()

        if df.empty:
            return self._handle_empty_scan(init, output_col)

        state = init
        results = []

        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            try:
                state = func(state, row_dict)
                results.append(state)
            except Exception as e:
                raise RuntimeError(
                    f"State transition function failed at row {idx}: {e}"
                ) from e

        df[output_col] = results
        rows = df.to_dict("records")

        result_schema = self._schema.copy()
        result_schema[output_col] = self._infer_type_from_value(
            results[0] if results else init
        )

        result = LTSeq._from_rows(rows, result_schema)
        result._sort_keys = self._sort_keys
        return result

    def _handle_empty_scan(self, init: Any, output_col: str) -> "LTSeq":
        """Handle stateful_scan on empty table."""
        from .core import LTSeq

        result_schema = self._schema.copy()
        result_schema[output_col] = self._infer_type_from_value(init)
        return LTSeq._from_rows([], result_schema)

    def _infer_type_from_value(self, value: Any) -> str:
        """Infer schema type from a Python value."""
        if isinstance(value, bool):
            return "bool"
        elif isinstance(value, int):
            return "int64"
        elif isinstance(value, float):
            return "float64"
        elif isinstance(value, str):
            return "string"
        else:
            return "object"
