"""Join operations for LTSeq: join, asof_join, semi_join, anti_join."""

from typing import Any, Callable, TYPE_CHECKING

from ._typing import LTSeqLike
if TYPE_CHECKING:
    from .core import LTSeq

from .expr import SchemaProxy


def _column_key_dict(name: str) -> dict[str, Any]:
    """Serialized single-column key expression, matching ColumnExpr.serialize()."""
    return {"type": "Column", "name": name}


def _and_key_dict(names: list[str], other_names: list[str]) -> dict[str, Any]:
    """Build the And-tree of Eq(col, col) that the Rust join path expects for
    composite string keys (left_names[i] == right_names[i])."""
    eqs = [
        {"type": "BinOp", "op": "Eq",
         "left": _column_key_dict(ln), "right": _column_key_dict(rn)}
        for ln, rn in zip(names, other_names)
    ]
    tree = eqs[0]
    for eq in eqs[1:]:
        tree = {"type": "BinOp", "op": "And", "left": tree, "right": eq}
    return tree


def _collect_eq_pairs(node: dict[str, Any], out: list[tuple[str, str]]) -> bool:
    """Walk an And-tree of Eq(column, column), appending (left, right) name pairs
    in tree order. Returns False if any leaf isn't a plain column equality."""
    if node.get("type") != "BinOp":
        return False
    op = node.get("op")
    if op == "And":
        return _collect_eq_pairs(node.get("left", {}), out) and _collect_eq_pairs(
            node.get("right", {}), out
        )
    if op == "Eq":
        left, right = node.get("left", {}), node.get("right", {})
        if left.get("type") == "Column" and right.get("type") == "Column":
            out.append((left["name"], right["name"]))
            return True
    return False


def _key_col_pairs(
    left_key_expr: dict[str, Any], right_key_expr: dict[str, Any]
) -> "list[tuple[str, str]] | None":
    """Ordered (left_col, right_col) join-key pairs from serialized key exprs, or
    None if the shape isn't a plain column / And-of-column-equalities (e.g. a
    non-equi or computed key). Single-column keys arrive as separate Column
    dicts; composite keys arrive as one shared And-tree on both sides."""
    if left_key_expr.get("type") == "Column" and right_key_expr.get("type") == "Column":
        return [(left_key_expr["name"], right_key_expr["name"])]
    if left_key_expr.get("type") == "BinOp" and left_key_expr.get("op") == "And":
        pairs: list[tuple[str, str]] = []
        if _collect_eq_pairs(left_key_expr, pairs):
            return pairs
    return None


def _resolve_join_keys(
    on: "Callable | str | list[str] | None",
    left_on: "str | list[str] | None",
    right_on: "str | list[str] | None",
    self_schema: dict[str, str],
    other_schema: dict[str, str],
    how: str,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Resolve join keys from the string/list shortcuts or the lambda form.

    Returns (left_key_expr, right_key_expr, join_type) in the same serialized
    shape the Rust join path consumes, so string keys and lambda keys converge.
    """
    # Column-name shortcuts (Pandas/Polars style) take precedence when given.
    if left_on is not None or right_on is not None:
        if on is not None:
            raise ValueError("Pass either on= or left_on=/right_on=, not both")
        if left_on is None or right_on is None:
            raise ValueError("left_on= and right_on= must be given together")
        left_cols = [left_on] if isinstance(left_on, str) else list(left_on)
        right_cols = [right_on] if isinstance(right_on, str) else list(right_on)
        if len(left_cols) != len(right_cols):
            raise ValueError("left_on and right_on must have the same length")
        _check_cols(left_cols, self_schema, "left")
        _check_cols(right_cols, other_schema, "right")
    elif isinstance(on, (str, list)):
        cols = [on] if isinstance(on, str) else list(on)
        _check_cols(cols, self_schema, "left")
        _check_cols(cols, other_schema, "right")
        left_cols = right_cols = cols
    else:
        # Lambda form (or None) — delegate to the existing extractor.
        from .helpers import _extract_join_keys

        if on is None:
            raise ValueError("join() requires on=, or left_on=/right_on=")
        return _extract_join_keys(on, self_schema, other_schema, how)

    if len(left_cols) == 1:
        return _column_key_dict(left_cols[0]), _column_key_dict(right_cols[0]), how
    tree = _and_key_dict(left_cols, right_cols)
    return tree, tree, how


def _check_cols(cols: list[str], schema: dict[str, str], side: str) -> None:
    for c in cols:
        if c not in schema:
            raise ValueError(
                f"Join column '{c}' not found in {side} table. "
                f"Available columns: {list(schema.keys())}"
            )


def _validate_join_inputs(self_table: Any, other: Any, how: str) -> None:
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


class JoinMixin(LTSeqLike):
    """Mixin class providing join operations for LTSeq."""

    def _execute_join(
        self,
        other: "LTSeq",
        left_key_expr: dict,
        right_key_expr: dict,
        jtype: str,
        suffix: str,
        method_name: str = "join",
    ) -> "LTSeq":
        """Core join implementation (keys already resolved)."""
        from .core import LTSeq

        try:
            joined_inner = self._inner.join(
                other._inner, left_key_expr, right_key_expr, jtype, suffix
            )
        except RuntimeError as e:
            raise RuntimeError(
                f"Rust {method_name} failed: {e}. "
                f"This typically indicates an unsupported join expression. "
                f"Simplify your join condition (e.g., use lambda a, b: a.col == b.col)."
            ) from e

        return LTSeq._from_inner(joined_inner)

    def join(
        self,
        other: "LTSeq",
        on: "Callable | str | list[str] | None" = None,
        how: str = "inner",
        strategy: str | None = None,
        *,
        left_on: "str | list[str] | None" = None,
        right_on: "str | list[str] | None" = None,
        suffix: str = "_right",
    ) -> "LTSeq":
        """
        Join two tables with configurable strategy.

        Args:
            other: Another LTSeq table to join with
            on: Join key(s). A column name string or list of names for an
                equi-join (Pandas/Polars style), or a two-arg lambda equating
                columns — including differently-named or composite keys
                (e.g. lambda a, b: a.user_id == b.id, or
                lambda a, b: (a.region == b.region) & (a.year == b.year)).
                Only equality conditions are supported; for inequality/range
                matching use asof_join().
            how: Join type: "inner", "left", "right", "full"
            strategy: Join strategy (optional):
                - None: default hash join
                - "hash": explicit hash join (same as None)
                - "merge": merge join for pre-sorted tables (validates sort order)
            left_on / right_on: Column name(s) for differently-named keys
            suffix: Appended to right-table columns that collide with the left
                table (Polars semantics; default "_right"). For inner/left joins
                the duplicate right key column is dropped.

        Returns:
            Joined LTSeq with columns from both tables

        Raises:
            ValueError: If strategy="merge" and tables are not sorted by join keys

        Example:
            >>> users.join(orders, on="id", how="left")
            >>> users.join(orders, left_on="id", right_on="user_id", suffix="_o")
            >>> users.join(orders, on=lambda u, o: u.id == o.user_id)
        """
        valid_strategies = {None, "hash", "merge"}
        if strategy not in valid_strategies:
            raise ValueError(
                f"Invalid strategy '{strategy}'. Must be one of: None, 'hash', 'merge'"
            )

        _validate_join_inputs(self, other, how)
        if callable(on):
            try:
                _ = on(SchemaProxy(self._schema), SchemaProxy(other._schema))
            except Exception as e:
                raise TypeError(f"Invalid join condition: {e}")

        left_key_expr, right_key_expr, jtype = _resolve_join_keys(
            on, left_on, right_on, self._schema, other._schema, how
        )

        if strategy == "merge":
            return self._join_with_sort_validation(
                other, left_key_expr, right_key_expr, jtype, suffix
            )

        return self._execute_join(
            other, left_key_expr, right_key_expr, jtype, suffix, "join"
        )

    def _join_with_sort_validation(
        self,
        other: "LTSeq",
        left_key_expr: dict,
        right_key_expr: dict,
        jtype: str,
        suffix: str,
    ) -> "LTSeq":
        """Merge join with sort order validation (used by strategy='merge')."""
        from .core import LTSeq

        # Extract every join-key column pair — single columns AND composite
        # And-trees (from on=["a","b"] or a lambda `(a==x) & (b==y)`). Reading
        # only a top-level "name" would miss composite keys and silently accept
        # unsorted tables. (pairs is None only for shapes merge join can't
        # consume, e.g. computed keys — leave those to the Rust path to reject.)
        pairs = _key_col_pairs(left_key_expr, right_key_expr)

        if pairs:
            left_cols = [lc for lc, _ in pairs]
            right_cols = [rc for _, rc in pairs]
            left_descs = [self._get_sort_direction(c) for c in left_cols]
            right_descs = [other._get_sort_direction(c) for c in right_cols]

            # Merge join needs each table sorted by the FULL key tuple in key
            # order (is_sorted_by does ordered prefix matching), not each column
            # independently.
            from .exceptions import SortRequiredError

            left_expected = [d if d is not None else False for d in left_descs]
            if not self.is_sorted_by(*left_cols, desc=left_expected):
                cols = "', '".join(left_cols)
                raise SortRequiredError(
                    f"Left table is not sorted by '{cols}'. "
                    f"Use .sort(...) first or use join() without strategy instead."
                )

            right_expected = [d if d is not None else False for d in right_descs]
            if not other.is_sorted_by(*right_cols, desc=right_expected):
                cols = "', '".join(right_cols)
                raise SortRequiredError(
                    f"Right table is not sorted by '{cols}'. "
                    f"Use .sort(...) first or use join() without strategy instead."
                )

            # Directions must match per key pair.
            for (lc, rc), ld, rd in zip(pairs, left_descs, right_descs):
                if ld is not None and rd is not None and ld != rd:
                    raise ValueError(
                        f"Sort directions don't match: left '{lc}' is "
                        f"{'descending' if ld else 'ascending'}, "
                        f"right '{rc}' is "
                        f"{'descending' if rd else 'ascending'}."
                    )

        # Execute merge join
        try:
            joined_inner = self._inner.join(
                other._inner, left_key_expr, right_key_expr, jtype, suffix
            )
        except RuntimeError as e:
            raise RuntimeError(
                f"Rust merge join failed: {e}. "
                f"Ensure both tables are sorted by the join key, or use join() "
                f"without strategy='merge' for unsorted data."
            ) from e

        return LTSeq._from_inner(joined_inner)

    def _get_sort_direction(self, col_name: str) -> bool | None:
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
        on: "Callable | str | None" = None,
        direction: str | None = None,
        is_sorted: bool = False,
        *,
        left_on: str | None = None,
        right_on: str | None = None,
        by: "str | list[str] | None" = None,
        strategy: str | None = None,
        suffix: str = "_right",
    ) -> "LTSeq":
        """
        As-of join for time-series data (Polars join_asof aligned).

        For each left row, finds the nearest right row by a time column.

        Args:
            other: Another LTSeq table
            on: Time column name (same name on both sides), or a two-arg lambda
                condition (e.g., lambda t, q: t.time >= q.time). The lambda's
                comparison operator is authoritative for the match direction
            direction: Deprecated alias for strategy
            is_sorted: If True, skip sort verification (faster)
            left_on / right_on: Time column names when they differ
            by: Column name(s) to group by — each left row matches only right
                rows sharing the same by-value (per-symbol asof, etc.)
            strategy: "backward" (<=), "forward" (>=), or "nearest".
                Default "backward". With a lambda on=, the operator decides and
                strategy must agree if also given
            suffix: Appended to conflicting right columns (Polars semantics).
                The right time column is kept (matched timestamp is real info)

        Returns:
            Joined LTSeq

        Example:
            >>> trades.asof_join(quotes, on="time", by="symbol", strategy="backward")
            >>> trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)
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

        # strategy is the primary name; direction is a back-compat alias.
        if strategy is not None and direction is not None and strategy != direction:
            raise ValueError(
                f"asof_join() got conflicting strategy='{strategy}' and "
                f"direction='{direction}'; pass only one."
            )
        resolved = strategy if strategy is not None else direction

        valid_strategies = {"backward", "forward", "nearest"}
        if resolved is not None and resolved not in valid_strategies:
            raise ValueError(
                f"Invalid strategy '{resolved}'. Must be one of: {valid_strategies}"
            )

        # Resolve the time columns and (for lambda form) the authoritative
        # direction from the comparison operator.
        op_strategy = None
        if callable(on):
            if left_on is not None or right_on is not None:
                raise ValueError(
                    "asof_join() got a lambda on= together with left_on=/right_on=; "
                    "the lambda already names both time columns. Pass either a lambda "
                    "on=, or left_on=/right_on= (or a string on=), not both."
                )
            try:
                _ = on(SchemaProxy(self._schema), SchemaProxy(other._schema))
            except Exception as e:
                raise TypeError(f"Invalid asof join condition: {e}")
            from .helpers import _extract_asof_keys

            left_time_col, right_time_col, operator = _extract_asof_keys(
                on, self._schema, other._schema
            )
            # Ge/Gt (t.time >= q.time) → backward; Le/Lt → forward.
            op_strategy = "backward" if operator in ("Ge", "Gt") else "forward"
            # The lambda operator is authoritative for direction, so any explicit
            # strategy/direction must agree with it — including "nearest", which a
            # directional operator can contradict (a nearest match may land on the
            # opposite side of the predicate). Use the string on= form for "nearest".
            if resolved is not None and resolved != op_strategy:
                raise ValueError(
                    f"asof_join() lambda operator implies strategy='{op_strategy}', "
                    f"but strategy='{resolved}' was passed. Remove one (use the "
                    f"string on= form for 'nearest')."
                )
            resolved = resolved if resolved is not None else op_strategy
        else:
            if left_on is not None or right_on is not None:
                if on is not None:
                    raise ValueError("Pass either on= or left_on=/right_on=, not both")
                if left_on is None or right_on is None:
                    raise ValueError("left_on= and right_on= must be given together")
                left_time_col, right_time_col = left_on, right_on
            elif isinstance(on, str):
                left_time_col = right_time_col = on
            else:
                raise ValueError(
                    "asof_join() requires on= (a time column name or lambda), "
                    "or left_on=/right_on="
                )
            resolved = resolved if resolved is not None else "backward"

        for col_name, schema, side in (
            (left_time_col, self._schema, "left"),
            (right_time_col, other._schema, "right"),
        ):
            if col_name not in schema:
                raise ValueError(
                    f"asof_join time column '{col_name}' not found in {side} table. "
                    f"Available columns: {list(schema.keys())}"
                )

        # Normalize by= to matched left/right column lists.
        if by is None:
            left_by: list[str] = []
            right_by: list[str] = []
        else:
            by_cols = [by] if isinstance(by, str) else list(by)
            for c in by_cols:
                if c not in self._schema:
                    raise ValueError(f"asof_join by= column '{c}' not found in left table")
                if c not in other._schema:
                    raise ValueError(f"asof_join by= column '{c}' not found in right table")
            left_by = right_by = by_cols

        left_table = self
        right_table = other
        if not is_sorted:
            if not self.is_sorted_by(left_time_col):
                left_table = self.sort(left_time_col)
            if not other.is_sorted_by(right_time_col):
                right_table = other.sort(right_time_col)

        try:
            joined_inner = left_table._inner.asof_join(
                right_table._inner,
                left_time_col,
                right_time_col,
                resolved,
                suffix,
                left_by,
                right_by,
            )
        except RuntimeError as e:
            raise RuntimeError(f"Asof join failed: {e}")

        return LTSeq._from_inner(joined_inner)

    def _filtering_join(
        self, other: "LTSeq", on: "Callable | str | list[str]", join_type: str
    ) -> "LTSeq":
        """
        Shared implementation for semi-join and anti-join.

        Args:
            other: Right table to match against
            on: Column name(s) string/list for equi-keys, or a lambda condition
            join_type: "semi" or "anti"

        Returns:
            LTSeq with filtered rows from left table
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"{join_type}_join() argument must be LTSeq, got {type(other).__name__}"
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        if callable(on):
            try:
                _ = on(SchemaProxy(self._schema), SchemaProxy(other._schema))
            except Exception as e:
                raise TypeError(f"Invalid join condition: {e}")

        left_key_expr, right_key_expr, _ = _resolve_join_keys(
            on, None, None, self._schema, other._schema, "inner"
        )

        try:
            if join_type == "semi":
                joined_inner = self._inner.semi_join(
                    other._inner, left_key_expr, right_key_expr
                )
            else:
                joined_inner = self._inner.anti_join(
                    other._inner, left_key_expr, right_key_expr
                )
        except RuntimeError as e:
            raise RuntimeError(f"{join_type.capitalize()}-join failed: {e}")

        result = LTSeq._from_inner(joined_inner)
        return result

    def semi_join(self, other: "LTSeq", on: "Callable | str | list[str]") -> "LTSeq":
        """
        Semi-join: return rows from left table where keys exist in right table.

        Unlike regular joins, semi-join returns only left table columns,
        with no duplicates from multiple matches in the right table.

        Args:
            other: Right table to match against
            on: Lambda specifying join condition (e.g., lambda a, b: a.user_id == b.id)

        Returns:
            LTSeq with rows from left table that have matching keys in right

        Example:
            >>> # Users who have placed at least one order
            >>> active_users = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)
        """
        return self._filtering_join(other, on, "semi")

    def anti_join(self, other: "LTSeq", on: "Callable | str | list[str]") -> "LTSeq":
        """
        Anti-join: return rows from left table where keys do NOT exist in right table.

        Unlike regular joins, anti-join returns only left table columns,
        containing rows that have NO match in the right table.

        Args:
            other: Right table to match against
            on: Lambda specifying join condition (e.g., lambda a, b: a.user_id == b.id)

        Returns:
            LTSeq with rows from left table that have NO matching keys in right

        Example:
            >>> # Users who have never placed an order
            >>> inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
        """
        return self._filtering_join(other, on, "anti")
