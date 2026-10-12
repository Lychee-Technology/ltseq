"""Copy-on-write row-level mutation operations for LTSeq.

Delegates to Rust-native implementations for maximum performance.
All operations return new LTSeq instances — the original is never modified.
"""

from typing import Any, Callable, Mapping, TYPE_CHECKING

from ._typing import LTSeqLike
from .exceptions import ColumnNotFoundError
from .expr.core_types import _encode_literal

if TYPE_CHECKING:
    from .core import LTSeq


def _literal_values(
    schema: Mapping[str, str], values: Mapping[Any, Any]
) -> dict[Any, dict[str, Any]]:
    """Each value as the typed payload a literal in a lambda is sent as (#194).

    The engine decodes it as it decodes an expression literal and places it
    into the column's type, so a value written here means what the same
    value means in a filter. A value of a type no literal has raises here,
    naming the column. Every key is checked against ``schema`` first, so a
    key that is not a column raises ``ColumnNotFoundError`` whatever the
    values are.
    """
    for name in values:
        if name not in schema:
            raise ColumnNotFoundError(
                f"Column '{name}' not found in schema. "
                f"Available columns: {list(schema)}"
            )
    payloads = {}
    for name, value in values.items():
        try:
            payload = _encode_literal(value, membership_hint=False)
        except TypeError as error:
            raise TypeError(f"column {name!r}: {error}") from None
        except ValueError as error:
            raise ValueError(f"column {name!r}: {error}") from None
        payloads[name] = {"type": "Literal", **payload}
    return payloads


class MutationMixin(LTSeqLike):
    """Mixin providing copy-on-write row mutation operations for LTSeq."""

    def insert(self, pos: int, row_dict: dict[str, Any]) -> "LTSeq":
        """
        Insert a row at the given 0-based position (copy-on-write).

        If pos is beyond the end of the table it is clamped to the table length
        (appending).  Negative pos is clamped to 0.

        Args:
            pos: 0-based insertion index
            row_dict: Dict mapping column names to values for the new row.
                A column without a value is NULL. Each value is a literal,
                read as the same value in a lambda is, and must be exact in
                the column's type, which never changes.

        Returns:
            New LTSeq with the row inserted; the original is unchanged.

        Raises:
            TypeError: ``row_dict`` is not a mapping, or a value's type is not
                a supported literal type.
            ValueError: A key is not a column (``ColumnNotFoundError``, raised
                before any value is checked), or the column cannot hold a
                value exactly: ``1.5`` or ``True`` for an integer column,
                ``Decimal("1.234")`` for ``decimal(5, 2)``, ``1e39`` for a
                ``float32`` column, a number for a date or string column.

        Example:
            >>> t2 = t.insert(0, {"id": 99, "name": "Alice"})   # prepend
            >>> t2 = t.insert(len(t), {"id": 99, "name": "Bob"}) # append
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(row_dict, Mapping):
            raise TypeError(
                f"insert() takes a dict of column values, got {type(row_dict).__name__}"
            )
        result_inner = self._inner.insert_row(pos, _literal_values(self._schema, row_dict))
        result = LTSeq._from_inner(result_inner)
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
            # Capture the user's predicate directly (so the `is None` rewrite
            # and its guidance apply) and negate the captured expression;
            # wrapping the predicate in another lambda would bypass both.
            expr_dict = self._capture_expr(predicate_or_pos)
            if expr_dict.get("type") == "Dict":
                raise TypeError(
                    "delete() predicate must return a boolean expression, "
                    "got a dict"
                )
            negated = {"type": "UnaryOp", "op": "Not", "operand": expr_dict}
            return LTSeq._from_inner(self._inner.filter(negated))

        # integer positional delete — delegate to Rust
        pos = int(predicate_or_pos)
        result_inner = self._inner.delete_rows(pos)
        result = LTSeq._from_inner(result_inner)
        return result

    def update(self, predicate: Callable, **updates: Any) -> "LTSeq":
        """
        Conditionally update column values where predicate is True (copy-on-write).

        For each (column, new_value) pair: the column becomes
        ``if_else(predicate(r), new_value, old_column_value)``.

        Args:
            predicate: Lambda returning a boolean expression (same style as filter())
            **updates: Keyword arguments mapping column_name -> new_value. Each
                value is a literal, written under the rules of ``insert``;
                expressions are not supported.

        Returns:
            New LTSeq with matching rows updated; the original is unchanged.

        Raises:
            TypeError, ValueError: As for ``insert``.

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
            from .core import LTSeq

            result = LTSeq._from_inner(self._inner)
            return result

        expr_dict = self._capture_expr(predicate)
        result_inner = self._inner.conditional_update(expr_dict, _literal_values(self._schema, updates))
        result = LTSeq._from_inner(result_inner)
        return result

    def modify(self, pos: int, **updates: Any) -> "LTSeq":
        """
        Modify specific columns in the single row at 0-based position pos (copy-on-write).

        Silently ignored if pos is out of range; the values are checked either way.

        Args:
            pos: 0-based row index
            **updates: Keyword arguments mapping column_name -> new_value. Each
                value is a literal, written under the rules of ``insert``.

        Returns:
            New LTSeq with the row modified; the original is unchanged.

        Raises:
            TypeError, ValueError: As for ``insert``.

        Example:
            >>> t2 = t.modify(0, status="active", score=100)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        result_inner = self._inner.modify_row(pos, _literal_values(self._schema, updates))
        result = LTSeq._from_inner(result_inner)
        return result
