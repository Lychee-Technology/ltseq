"""Copy-on-write row-level mutation operations for LTSeq.

Delegates to Rust-native implementations for maximum performance.
All operations return new LTSeq instances — the original is never modified.
"""

from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .core import LTSeq


class MutationMixin:
    """Mixin providing copy-on-write row mutation operations for LTSeq."""

    def insert(self, pos: int, row_dict: dict[str, Any]) -> "LTSeq":
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
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        result_inner = self._inner.insert_row(pos, row_dict)
        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def delete(self, predicate_or_pos: Callable | int) -> "LTSeq":
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

        # integer positional delete — delegate to Rust
        pos = int(predicate_or_pos)
        result_inner = self._inner.delete_rows(pos)
        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def update(self, predicate: Callable, **updates: Any) -> "LTSeq":
        """
        Conditionally update column values where predicate is True (copy-on-write).

        For each (column, new_value) pair: the column becomes
        ``if_else(predicate(r), new_value, old_column_value)``.

        Args:
            predicate: Lambda returning a boolean expression (same style as filter())
            **updates: Keyword arguments mapping column_name -> new_value (literal or Expr)

        Returns:
            New LTSeq with matching rows updated; the original is unchanged.

        Example:
            >>> t2 = t.update(lambda r: r.age > 65, discount=0.2)
            >>> t2 = t.update(lambda r: r.status == "old", status="archived")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not updates:
            return self

        expr_dict = self._capture_expr(predicate)
        result_inner = self._inner.conditional_update(expr_dict, updates)
        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def modify(self, pos: int, **updates: Any) -> "LTSeq":
        """
        Modify specific columns in the single row at 0-based position pos (copy-on-write).

        Silently ignored if pos is out of range.

        Args:
            pos: 0-based row index
            **updates: Keyword arguments mapping column_name -> new_value

        Returns:
            New LTSeq with the row modified; the original is unchanged.

        Example:
            >>> t2 = t.modify(0, status="active", score=100)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        result_inner = self._inner.modify_row(pos, updates)
        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result