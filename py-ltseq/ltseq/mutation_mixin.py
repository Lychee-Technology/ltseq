"""Copy-on-write row-level mutation operations for LTSeq."""

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
        import pyarrow as pa

        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        pa_table = self.to_arrow()
        pos = max(0, min(pos, len(pa_table)))

        # Build single-row Arrow table matching the existing schema.
        # Infer type from the Python value first, then cast to the column type
        # so that implicit coercions (e.g. "0.00" str → float64) work correctly.
        new_row_arrays = {
            field.name: pa.array([row_dict.get(field.name)]).cast(field.type)
            for field in pa_table.schema
        }
        new_row = pa.table(new_row_arrays, schema=pa_table.schema)

        result_table = pa.concat_tables(
            [pa_table.slice(0, pos), new_row, pa_table.slice(pos)]
        )
        result = LTSeq.from_arrow(result_table)
        result._schema = self._schema
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
        import pyarrow as pa

        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if callable(predicate_or_pos):
            pred = predicate_or_pos
            return self.filter(lambda r, _p=pred: ~_p(r))

        # integer positional delete
        pos = int(predicate_or_pos)
        pa_table = self.to_arrow()

        if 0 <= pos < len(pa_table):
            # Keep rows before and after pos
            pa_table = pa.concat_tables(
                [pa_table.slice(0, pos), pa_table.slice(pos + 1)]
            ) if pos < len(pa_table) - 1 else pa_table.slice(0, pos)

        result = LTSeq.from_arrow(pa_table)
        result._schema = self._schema
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

        import pyarrow as pa
        import pyarrow.compute as pc

        from .core import LTSeq

        # Derive a boolean mask column, then apply updates using Arrow compute
        _MASK = "__ltseq_update_mask__"
        with_mask = self.derive(**{_MASK: predicate})
        pa_table = with_mask.to_arrow()

        mask_idx = pa_table.schema.get_field_index(_MASK)
        mask_col = pa_table.column(mask_idx).cast(pa.bool_())
        pa_table = pa_table.remove_column(mask_idx)

        for col_name, new_val in updates.items():
            idx = pa_table.schema.get_field_index(col_name)
            if idx >= 0:
                new_arr = pc.if_else(mask_col, new_val, pa_table.column(idx))
                pa_table = pa_table.set_column(idx, col_name, new_arr)

        result = LTSeq.from_arrow(pa_table)
        result._schema = self._schema
        result._sort_keys = None
        return result

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
        import pyarrow as pa

        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        pa_table = self.to_arrow()

        if 0 <= pos < len(pa_table):
            for col_name, val in updates.items():
                idx = pa_table.schema.get_field_index(col_name)
                if idx >= 0:
                    col_list = pa_table.column(idx).to_pylist()
                    col_list[pos] = val
                    field_type = pa_table.schema.field(col_name).type
                    pa_table = pa_table.set_column(
                        idx, col_name, pa.array(col_list, type=field_type)
                    )

        result = LTSeq.from_arrow(pa_table)
        result._schema = self._schema
        result._sort_keys = None
        return result
