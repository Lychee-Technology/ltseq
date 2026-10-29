"""Lookup mixin for handling lookup expressions in derive operations."""

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .core import LTSeq


def _bare_lookup(col_name: str, expr_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Return the Lookup node of a derived column that is ``r.<column>.lookup(...)``.

    derive() joins the target table on the key column and replaces the
    derived column with the joined target column. That evaluates the lookup
    only when it is the column's whole expression and its key is a column; for
    any other shape it would drop the rest of the expression, so it raises
    instead (#223).

    Raises:
        ValueError: If the lookup is inside a larger expression or its key is
            not a column
    """
    if expr_dict.get("type") != "Lookup":
        raise ValueError(
            f"Column {col_name!r}: lookup() is supported only as the whole "
            "expression of a derived column, not inside a larger expression. "
            "Derive the looked-up value as a column of its own and build on it "
            "in a later derive(), or use join()."
        )
    if expr_dict["on"].get("type") != "Column":
        raise ValueError(
            f"Column {col_name!r}: lookup() needs a column as its key, as in "
            "r.<column>.lookup(...), not a computed value. Derive the key as a "
            "column first and look it up in a later derive(), or use join()."
        )
    return expr_dict


class LookupMixin:
    """Mixin providing lookup resolution for derive operations."""

    # These will be provided by the main class (lazy properties on LTSeq)
    _inner: Any

    @property
    def _schema(self) -> dict[str, str]: ...

    @property
    def _sort_keys(self) -> Any: ...

    def _contains_lookup(self, expr_dict: Any) -> bool:
        """
        Recursively check if expression contains a Lookup.

        Args:
            expr_dict: Serialized expression dict

        Returns:
            True if expression contains a Lookup type
        """
        # Walk every field, so a lookup under a window, an argument list or
        # any other node is found and _bare_lookup can reject it.
        if isinstance(expr_dict, dict):
            return expr_dict.get("type") == "Lookup" or any(
                self._contains_lookup(value) for value in expr_dict.values()
            )
        if isinstance(expr_dict, list):
            return any(self._contains_lookup(item) for item in expr_dict)
        return False

    def _resolve_lookups(
        self, derived_cols: dict[str, dict]
    ) -> tuple["LTSeq", dict[str, dict]]:
        """
        Resolve lookup expressions by joining with target tables.

        Each distinct lookup (target table, key column, target key) is one
        left join, which adds every target column as ``{alias}_{column}``;
        a lookup column is rewritten to reference its joined column. No
        joined column has the name of an input or derived column, so the
        caller can drop every column of the joined table that its input does
        not have.

        Args:
            derived_cols: Dict mapping column names to serialized expressions

        Returns:
            Tuple of (joined_table, simplified_derived_cols)
            - joined_table: LTSeq with target tables joined in
            - simplified_derived_cols: Expressions rewritten to use joined columns
        """
        from .exceptions import ColumnNotFoundError
        from .expr.types import LookupExpr

        current = cast("LTSeq", self)
        simplified = {}
        # Alias of the join for each (target, key column, target key)
        aliases: dict[tuple[str, str, str], str] = {}
        n = 0  # number of the next alias to try

        for col_name, expr_dict in derived_cols.items():
            if not self._contains_lookup(expr_dict):
                # No lookup in this expression, keep as-is
                simplified[col_name] = expr_dict
                continue

            lookup_info = _bare_lookup(col_name, expr_dict)
            target_name = lookup_info["target_name"]
            target_table = LookupExpr.get_table(target_name)

            if target_table is None:
                raise ValueError(
                    f"Lookup target table '{target_name}' not found. "
                    "Make sure the table is passed to lookup()."
                )

            on_column = lookup_info["on"]["name"]
            [target_col] = lookup_info["target_columns"]
            target_schema = target_table._schema
            if target_col not in target_schema:
                raise ColumnNotFoundError(
                    f"Column {col_name!r}: the lookup() target has no column "
                    f"{target_col!r}. Available columns: {list(target_schema)}"
                )

            join_key = lookup_info["join_key"]
            if join_key is None:
                # Default: the key column's own name, or the target's first column
                if on_column in target_schema:
                    join_key = on_column
                elif target_schema:
                    join_key = next(iter(target_schema))
                else:
                    raise ValueError(
                        f"Cannot determine join key for lookup on '{target_name}'. "
                        f"Please specify join_key explicitly."
                    )
            elif join_key not in target_schema:
                raise ColumnNotFoundError(
                    f"Column {col_name!r}: the lookup() target has no join_key column "
                    f"{join_key!r}. Available columns: {list(target_schema)}"
                )

            join = (target_name, on_column, join_key)
            if join not in aliases:
                # Skip an alias that would give a joined column the name of a
                # column of the table (the input's or an earlier join's) or of
                # a derived one: the join would fail as ambiguous, or the
                # derived column would take the joined column's place.
                taken = set(current._schema) | set(derived_cols)
                while any(f"_lkp{n}_{col}" in taken for col in target_schema):
                    n += 1
                aliases[join] = f"_lkp{n}"
                n += 1
                current = current._perform_lookup_join(
                    target_table, on_column, join_key, aliases[join]
                )

            # Rust join creates columns as {alias}_{column}
            simplified[col_name] = {
                "type": "Column",
                "name": f"{aliases[join]}_{target_col}",
            }

        return current, simplified

    def _perform_lookup_join(
        self,
        target_table: "LTSeq",
        on_column: str,
        join_key: str,
        alias: str,
    ) -> "LTSeq":
        """
        Perform a left join with a target table for lookup resolution.

        The join adds every target column as ``{alias}_{column}``, and a key
        without a match gets NULL. A target that holds a key in more than one
        row would repeat each input row that matches it, so the joined table
        raises when it executes instead (#351).

        Args:
            target_table: The table to join with
            on_column: Column name in self to join on
            join_key: Column name in target_table to join on
            alias: Prefix for target table columns

        Returns:
            New LTSeq with joined data
        """
        from .core import LTSeq

        result_inner = self._inner.lookup_join(
            target_table._inner, on_column, join_key, alias
        )

        # The executed join's real schema comes from the Rust kernel.
        return LTSeq._from_inner(result_inner)
