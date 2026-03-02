"""Copy-on-write row-level mutation operations for LTSeq."""

from typing import Any, Callable, Dict


class MutationMixin:
    """Mixin providing copy-on-write row mutation operations for LTSeq."""

    def insert(self, pos: int, row_dict: Dict[str, Any]) -> "LTSeq":
        """
        Insert a row at the given 0-based position (copy-on-write).

        If pos is beyond the end of the table it is clamped to the table length
        (appending).  Negative pos is clamped to 0.

        Args:
            pos: 0-based insertion index
            row_dict: Dict mapping column names to values for the new row

        Returns:
            New LTSeq with the row inserted; the original is unchanged.

        Example:
            >>> t2 = t.insert(0, {"id": 99, "name": "Alice"})   # prepend
            >>> t2 = t.insert(len(t), {"id": 99, "name": "Bob"}) # append
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("insert() requires pandas. Install with: pip install pandas")

        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        df = self.to_pandas()
        pos = max(0, min(pos, len(df)))

        new_row = pd.DataFrame([row_dict])
        result_df = pd.concat(
            [df.iloc[:pos], new_row, df.iloc[pos:]]
        ).reset_index(drop=True)

        rows = result_df.to_dict("records")
        result = LTSeq._from_rows(rows, self._schema)
        result._sort_keys = None
        return result

    def delete(self, predicate_or_pos) -> "LTSeq":
        """
        Delete rows matching a predicate or the single row at a 0-based index.

        Args:
            predicate_or_pos:
                - callable: delete all rows where predicate returns True
                - int: delete the row at that 0-based position (silently ignored if out of range)

        Returns:
            New LTSeq with row(s) removed; the original is unchanged.

        Example:
            >>> t2 = t.delete(lambda r: r.status == "deleted")
            >>> t2 = t.delete(0)   # remove first row
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if callable(predicate_or_pos):
            pred = predicate_or_pos
            return self.filter(lambda r, _p=pred: ~_p(r))

        # integer positional delete
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("delete() with int pos requires pandas.")

        pos = int(predicate_or_pos)
        df = self.to_pandas()

        if 0 <= pos < len(df):
            df = df.drop(index=df.index[pos]).reset_index(drop=True)

        rows = df.to_dict("records")
        result = LTSeq._from_rows(rows, self._schema)
        result._sort_keys = None
        return result

    def update(self, predicate: Callable, **updates: Any) -> "LTSeq":
        """
        Conditionally update column values where predicate is True (copy-on-write).

        For each (column, new_value) pair: the column becomes
        ``if_else(predicate(r), new_value, old_column_value)``.

        Args:
            predicate: Lambda returning a boolean expression (same style as filter())
            **updates: Keyword arguments mapping column_name → new_value (literal or Expr)

        Returns:
            New LTSeq with matching rows updated; the original is unchanged.

        Example:
            >>> t2 = t.update(lambda r: r.age > 65, discount=0.2)
            >>> t2 = t.update(lambda r: r.status == "old", status="archived")
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not updates:
            return self

        from .expr.base import if_else

        derive_kwargs = {}
        for col_name, new_val in updates.items():
            def _make_expr(c: str, v: Any, p: Callable) -> Callable:
                return lambda r, _c=c, _v=v, _p=p: if_else(_p(r), _v, getattr(r, _c))

            derive_kwargs[col_name] = _make_expr(col_name, new_val, predicate)

        return self.derive(**derive_kwargs)

    def modify(self, pos: int, **updates: Any) -> "LTSeq":
        """
        Modify specific columns in the single row at 0-based position pos (copy-on-write).

        Silently ignored if pos is out of range.

        Args:
            pos: 0-based row index
            **updates: Keyword arguments mapping column_name → new_value

        Returns:
            New LTSeq with the row modified; the original is unchanged.

        Example:
            >>> t2 = t.modify(0, status="active", score=100)
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("modify() requires pandas. Install with: pip install pandas")

        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        df = self.to_pandas()

        if 0 <= pos < len(df):
            for col_name, val in updates.items():
                if col_name in df.columns:
                    df.iloc[pos, df.columns.get_loc(col_name)] = val

        rows = df.to_dict("records")
        result = LTSeq._from_rows(rows, self._schema)
        result._sort_keys = None
        return result
