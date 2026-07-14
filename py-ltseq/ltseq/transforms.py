"""Transform operations for LTSeq: filter, select, derive, sort, distinct, slice."""

from typing import Any, Callable, TYPE_CHECKING, cast

from ._typing import LTSeqLike
if TYPE_CHECKING:
    from .core import LTSeq

from .expr import SchemaProxy
from .lookup import LookupMixin


# Computed sort keys cannot be represented in column-level sort metadata
# (sort() truncates at the first computed key), so a table sorted ONLY by
# computed keys still has no declared order. Guards append this hint so the
# error is actionable for users who literally just called .sort(lambda ...).
_COMPUTED_SORT_HINT = (
    " Note: computed sort keys (e.g. sort(lambda r: r.a * 2)) do not "
    "establish a declared order — derive the key as a column first, "
    "e.g. .derive(k=...).sort('k')."
)


def _window_sort_required_error() -> Exception:
    """Build the SortRequiredError raised when a window expression is used
    on a table with no defined row order (neither ``.sort()`` nor
    ``.assume_sorted()`` has been applied)."""
    from .exceptions import SortRequiredError

    return SortRequiredError(
        "Window functions (shift/rolling/diff/rank/cumulative) require a "
        "defined row order. Call .sort(...) or .assume_sorted(...) before "
        "using them in derive()." + _COMPUTED_SORT_HINT
    )


def _process_select_col(col: str | Callable, schema: dict[str, str]) -> dict[str, Any] | list[dict[str, Any]]:
    """Process a single column argument for select()."""
    if isinstance(col, str):
        if col not in schema:
            raise AttributeError(
                f"Column '{col}' not found in schema. "
                f"Available columns: {list(schema.keys())}"
            )
        return {"type": "Column", "name": col}
    elif callable(col):
        proxy = SchemaProxy(schema)
        result = col(proxy)

        if isinstance(result, list):
            exprs = []
            for item in result:
                if hasattr(item, "serialize"):
                    exprs.append(item.serialize())
                else:
                    raise TypeError(
                        f"List items must be column expressions, got {type(item)}"
                    )
            return exprs
        elif hasattr(result, "serialize"):
            return result.serialize()
        else:
            raise TypeError(
                f"Lambda must return Expr(s) or list of Exprs, got {type(result)}"
            )
    else:
        raise TypeError(
            f"select() argument must be str or callable, got {type(col).__name__}"
        )


def _extract_derive_cols(
    args: tuple[Any, ...],
    kwargs: dict[str, Callable],
    schema: dict[str, str],
    capture_expr_fn: Callable,
) -> dict[str, dict[str, Any]]:
    """Extract derived columns from derive() arguments."""
    if len(args) > 0:
        if len(args) > 1:
            raise TypeError(
                f"derive() takes at most 1 positional argument ({len(args)} given)"
            )
        if kwargs:
            raise TypeError(
                "derive() cannot use both positional argument and keyword arguments"
            )

        func = cast(Callable, next(iter(args)))
        if not callable(func):
            raise TypeError(
                f"derive() positional argument must be callable, got {type(func).__name__}"
            )

        expr_dict = capture_expr_fn(func)

        if expr_dict.get("type") == "Dict":
            derived_cols = {}
            for key_expr, value_expr in zip(
                expr_dict.get("keys", []), expr_dict.get("values", [])
            ):
                if key_expr.get("type") == "Literal":
                    col_name = key_expr.get("value", "")
                    derived_cols[col_name] = value_expr
                else:
                    raise ValueError("Dict keys in derive() must be string literals")
        else:
            derived_cols = {"_derived": expr_dict}
    else:
        derived_cols = {}
        for col_name, fn in kwargs.items():
            if not callable(fn):
                raise TypeError(
                    f"derive() argument '{col_name}' must be callable, "
                    f"got {type(fn).__name__}"
                )
            derived_cols[col_name] = capture_expr_fn(fn)

    return derived_cols


def _collect_key_exprs(
    key_exprs: tuple | list,
    schema: dict[str, str],
    capture_expr_fn: Callable,
) -> list[dict[str, Any]]:
    """Collect key expressions from str or Callable arguments."""
    result = []
    for key_expr in key_exprs:
        if isinstance(key_expr, str):
            result.append({"type": "Column", "name": key_expr})
        elif callable(key_expr):
            expr_dict = capture_expr_fn(key_expr)
            result.append(expr_dict)
        else:
            raise TypeError(
                f"Argument must be str or callable, got {type(key_expr).__name__}"
            )
    return result


class _FoldRow:
    """Read-only view of a row passed to ``fold``'s callback.

    Supports both attribute access (``row.col``) — matching the expression DSL
    idiom used elsewhere in LTSeq — and item access (``row["col"]``). Any
    attempt to mutate the row raises, so a callback cannot leak changes into the
    original columns of the output.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict[str, Any]) -> None:
        object.__setattr__(self, "_data", data)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __getattr__(self, name: str) -> Any:
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(
                f"fold() row has no column '{name}'. "
                f"Available columns: {list(self._data.keys())}"
            ) from None

    def __setitem__(self, key: str, value: Any) -> None:
        raise TypeError("fold() row is read-only; return the new state instead of mutating the row")

    def __setattr__(self, name: str, value: Any) -> None:
        raise TypeError("fold() row is read-only; return the new state instead of mutating the row")

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def keys(self):
        return self._data.keys()

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"FoldRow({self._data!r})"


class TransformMixin(LookupMixin, LTSeqLike):
    """Mixin class providing transform operations for LTSeq."""

    def filter(self, predicate: Callable) -> "LTSeq":
        """
        Filter rows matching the predicate.

        Args:
            predicate: Lambda that returns boolean expression.
                      E.g., lambda r: r.amount > 100

        Returns:
            New LTSeq with filtered rows

        Raises:
            ValueError: If schema not initialized
            TypeError: If predicate invalid
            AttributeError: If column not found

        Example:
            >>> filtered = t.filter(lambda r: r.amount > 100)
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
        result_inner = self._inner.filter(expr_dict)

        return LTSeq._from_inner(result_inner)

    def select(self, *cols) -> "LTSeq":
        """
        Project specified columns or expressions.

        Args:
            *cols: Column names (str) or lambdas returning expressions

        Returns:
            New LTSeq with selected columns

        Example:
            >>> t.select("id", "name")
            >>> t.select(lambda r: [r.id, r.name])
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        exprs = []
        selected_cols = set()

        for col in cols:
            result = _process_select_col(col, self._schema)

            if isinstance(result, list):
                for expr in result:
                    exprs.append(expr)
                    if expr.get("type") == "Column" and "name" in expr:
                        selected_cols.add(expr["name"])
            else:
                exprs.append(result)
                if result.get("type") == "Column" and "name" in result:
                    selected_cols.add(result["name"])

        result_inner = self._inner.select(exprs)

        # Schema and sort metadata now come from the Rust kernel (issue #93);
        # select's sort-prefix retention lives in Rust's select_impl.
        return LTSeq._from_inner(result_inner)

    def derive(self, *args, **kwargs: Callable) -> "LTSeq":
        """
        Add or overwrite columns.

        Args:
            *args: Single callable returning dict of {col_name: expr}
            **kwargs: Mapping of column name to lambda

        Returns:
            New LTSeq with derived columns

        Example:
            >>> t.derive(tax=lambda r: r.price * 0.1)
            >>> t.derive(lambda r: {"tax": r.price * 0.1})
        """
        from .core import LTSeq
        from .expr.types import LookupExpr

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        derived_cols = _extract_derive_cols(
            args, kwargs, self._schema, self._capture_expr
        )

        # Check for lookup expressions (method from LookupMixin)
        has_lookup = any(self._contains_lookup(expr) for expr in derived_cols.values())

        if has_lookup:
            # Resolve lookups by performing joins (method from LookupMixin)
            try:
                resolved_table, simplified_cols = self._resolve_lookups(derived_cols)

                # Continue with derive on the joined table
                has_window = resolved_table._has_window_functions(simplified_cols)
                if (
                    resolved_table._has_implicit_order_window(simplified_cols)
                    and not resolved_table._sort_keys
                ):
                    raise _window_sort_required_error()
                if has_window:
                    result_inner = resolved_table._inner.derive_with_window_functions(
                        simplified_cols
                    )
                else:
                    result_inner = resolved_table._inner.derive(simplified_cols)

                return LTSeq._from_inner(result_inner)
            finally:
                # Always clear the registry after derive completes
                LookupExpr.clear_registry()

        # Normal derive path (no lookups)
        has_window = self._has_window_functions(derived_cols)

        if self._has_implicit_order_window(derived_cols) and not self._sort_keys:
            raise _window_sort_required_error()
        if has_window:
            result_inner = self._inner.derive_with_window_functions(
                derived_cols
            )
        else:
            result_inner = self._inner.derive(derived_cols)

        return LTSeq._from_inner(result_inner)

    # Alias for discoverability (Polars users expect with_columns)
    with_columns = derive

    def rename(self, mapping: dict[str, str] | None = None, **kwargs: str) -> "LTSeq":
        """
        Rename columns.

        Accepts either a dict mapping old names to new names, or keyword
        arguments where each key is the old name and the value is the new name.

        Args:
            mapping: Dictionary of {old_name: new_name}
            **kwargs: old_name=new_name keyword arguments

        Returns:
            New LTSeq with renamed columns

        Raises:
            ValueError: If schema not initialized
            KeyError: If any old column name not found in schema

        Example:
            >>> t.rename({"user_id": "uid", "event_time": "ts"})
            >>> t.rename(user_id="uid", event_time="ts")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        renames = {}
        if mapping is not None:
            if not isinstance(mapping, dict):
                raise TypeError(
                    f"mapping must be a dict, got {type(mapping).__name__}"
                )
            renames.update(mapping)
        renames.update(kwargs)

        if not renames:
            raise ValueError("rename() requires at least one column mapping")

        # Validate all old names exist
        for old_name in renames:
            if old_name not in self._schema:
                raise KeyError(
                    f"Column '{old_name}' not found in schema. "
                    f"Available columns: {list(self._schema.keys())}"
                )

        # Rust remaps sort metadata to the new names (a plain aliased select
        # cannot know it's a rename and would drop the renamed sort columns).
        result_inner = self._inner.rename_columns(renames)
        return LTSeq._from_inner(result_inner)

    def drop(self, *cols: str) -> "LTSeq":
        """
        Remove specified columns from the table.

        Args:
            *cols: Column names to drop

        Returns:
            New LTSeq without the specified columns

        Raises:
            ValueError: If schema not initialized or no columns specified
            KeyError: If any column not found in schema

        Example:
            >>> t.drop("temp_col", "debug_col")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not cols:
            raise ValueError("drop() requires at least one column name")

        cols_to_drop = set(cols)

        # Validate all columns exist
        for col in cols_to_drop:
            if col not in self._schema:
                raise KeyError(
                    f"Column '{col}' not found in schema. "
                    f"Available columns: {list(self._schema.keys())}"
                )

        # Build select expressions for remaining columns
        remaining_cols = [
            col for col in self._schema if col not in cols_to_drop
        ]

        if not remaining_cols:
            raise ValueError("Cannot drop all columns")

        select_exprs = [
            {"type": "Column", "name": col} for col in remaining_cols
        ]

        result_inner = self._inner.select(select_exprs)

        # Rust keeps the longest surviving prefix of the sort keys.
        return LTSeq._from_inner(result_inner)

    def _has_window_functions(self, derived_cols: dict[str, Any]) -> bool:
        """Check if derived columns contain window functions."""

        def check_expr(expr: dict[str, Any]) -> bool:
            if not isinstance(expr, dict):
                return False
            expr_type = expr.get("type", "")
            if expr_type == "Window":
                return True
            if expr_type == "Call":
                func = expr.get("func", "")
                if func in ("shift", "diff", "rolling", "cum_sum", "cum_max", "cum_min"):
                    return True
                if check_expr(expr.get("on") or {}):
                    return True
                for arg in expr.get("args", []):
                    if check_expr(arg):
                        return True
            elif expr_type == "BinOp":
                if check_expr(expr.get("left", {})):
                    return True
                if check_expr(expr.get("right", {})):
                    return True
            elif expr_type == "UnaryOp":
                if check_expr(expr.get("operand", {})):
                    return True
            return False

        for expr in derived_cols.values():
            if check_expr(expr):
                return True
        return False

    def _has_implicit_order_window(self, derived_cols: dict[str, Any]) -> bool:
        """Check for window functions that depend on the table's row order.

        These are shift/diff/rolling/cumulative expressions: unlike an explicit
        ``.over(order_by=...)`` window (``type == "Window"``, which carries its
        own ordering), they are only well-defined once the table has a defined
        order, so they require a prior ``.sort()`` / ``.assume_sorted()``.
        """

        def check_expr(expr: dict[str, Any]) -> bool:
            if not isinstance(expr, dict):
                return False
            expr_type = expr.get("type", "")
            if expr_type == "Window":
                # A .over() window is self-ordering only when it carries its own
                # order_by. A sequence window with order_by=None — e.g.
                # shift(1).over(partition_by=g) — still falls back to table order
                # in the Rust planner (window_native.rs::convert_window_sequence),
                # so it stays row-order bound and requires a prior .sort().
                if expr.get("order_by") is not None:
                    return False
                return check_expr(expr.get("expr") or {})
            if expr_type == "Call":
                func = expr.get("func", "")
                if func in ("shift", "diff", "rolling", "cum_sum", "cum_max", "cum_min"):
                    return True
                if check_expr(expr.get("on") or {}):
                    return True
                for arg in expr.get("args", []):
                    if check_expr(arg):
                        return True
            elif expr_type == "BinOp":
                if check_expr(expr.get("left", {})):
                    return True
                if check_expr(expr.get("right", {})):
                    return True
            elif expr_type == "UnaryOp":
                if check_expr(expr.get("operand", {})):
                    return True
            return False

        for expr in derived_cols.values():
            if check_expr(expr):
                return True
        return False

    def sort(
        self,
        *keys: str | Callable,
        desc: bool | list[bool] = False,
        descending: bool | list[bool] | None = None,
    ) -> "LTSeq":
        """
        Sort by one or more keys.

        Computed keys (e.g. ``lambda r: r.a * 2``) sort the data physically
        but cannot be tracked as declared order: sort metadata truncates at
        the first computed key, so a computed-only sort declares no order and
        ordered APIs (``cum_sum``, ``group_ordered``, window expressions)
        will still raise ``SortRequiredError``. Derive the key as a column
        first (``.derive(k=...).sort("k")``) to use it as declared order.
        After truncation, the order of rows tied on the declared prefix is
        unspecified.

        Args:
            *keys: Column names or expressions to sort by
            desc: Descending flag(s) - single bool or list per key
            descending: Alias for desc

        Returns:
            Sorted LTSeq with tracked sort keys

        Example:
            >>> t.sort("date", "id", desc=[False, True])
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if descending is not None:
            desc = descending

        if isinstance(desc, bool):
            desc_list = [desc] * len(keys)
        elif isinstance(desc, list):
            desc_list = desc
            if len(desc_list) != len(keys):
                raise ValueError(
                    f"desc list length ({len(desc_list)}) must match "
                    f"number of keys ({len(keys)})"
                )
        else:
            raise TypeError(f"desc must be bool or list, got {type(desc).__name__}")

        key_exprs = _collect_key_exprs(keys, self._schema, self._capture_expr)
        result_inner = self._inner.sort(key_exprs, desc_list)

        # Rust captures the sort specs (column + direction) itself.
        return LTSeq._from_inner(result_inner)

    def assume_sorted(
        self,
        *keys: str | Callable,
        desc: bool | list[bool] = False,
    ) -> "LTSeq":
        """
        Declare that data is already sorted by the given keys.

        Sets sort metadata without physically sorting. Use when reading
        pre-sorted data (e.g., pre-sorted Parquet files) to skip the
        sort overhead while enabling window functions and the Arrow
        shift fast path.

        The caller is responsible for ensuring the data is actually
        sorted in the declared order. Incorrect metadata will produce
        wrong results.

        Like sort(), the declared order truncates at the first computed
        key — only leading plain-column keys count.

        Args:
            *keys: Column names declaring the sort order
            desc: Descending flag(s) - single bool or list per key

        Returns:
            LTSeq with sort metadata set (same underlying data)

        Example:
            >>> t = LTSeq.read_parquet("presorted.parquet")
            >>> t = t.assume_sorted("userid", "eventtime")
            >>> t.is_sorted_by("userid")  # True
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() or read_parquet() first."
            )

        if isinstance(desc, bool):
            desc_list = [desc] * len(keys)
        elif isinstance(desc, list):
            desc_list = desc
            if len(desc_list) != len(keys):
                raise ValueError(
                    f"desc list length ({len(desc_list)}) must match "
                    f"number of keys ({len(keys)})"
                )
        else:
            raise TypeError(f"desc must be bool or list, got {type(desc).__name__}")

        key_exprs = _collect_key_exprs(keys, self._schema, self._capture_expr)
        result_inner = self._inner.assume_sorted(key_exprs, desc_list)

        return LTSeq._from_inner(result_inner)

    def fold(
        self,
        fn: Callable[[Any, "_FoldRow"], Any],
        *,
        init: Any,
        into: str,
        partition_by: "str | None" = None,
    ) -> "LTSeq":
        """
        Ordered stateful accumulation (fold / scan-left).

        Walks the rows in their current order, threading a running ``state``
        through ``fn(state, row)`` and appending the result as a new column
        ``into``. This expresses compounding, running balances, and small
        state machines that window functions cannot — the classic SPL-style
        capability.

        **Execution path**: unlike expressions (which push down to the Rust
        engine), ``fold`` runs a Python callback per row, so it materializes
        the whole table into Python and loses laziness. Prefer expression
        forms (``cum_sum``, ``shift``, ``when``) when they can express the
        computation; reach for ``fold`` only when genuinely sequential state
        is required. On large tables this is the slow path.

        The state is what ``fn`` returns each step; only the value written to
        ``into`` is materialized (the state and the ``into`` value are the same
        object here — return the value you want stored). Requires a prior
        ``sort()``/``assume_sorted()`` so the accumulation order is defined.

        Args:
            fn: ``fn(state, row) -> new_state``. ``row`` is a read-only view of
                the current row supporting both attribute (``row.rate``) and
                item (``row["rate"]``) access; mutating it raises. ``new_state``
                is stored in ``into`` and carried to the next row.
            init: Initial state, used before the first row of each partition.
            into: Name of the appended column holding the running state.
            partition_by: Optional column name; the state resets to ``init`` at
                the start of each partition (partitions follow first-seen order).

        Returns:
            A new in-memory LTSeq: the original rows in order, plus ``into``.

        Example:
            >>> # Compounding return
            >>> t.sort("date").fold(
            ...     lambda s, r: s * (1 + r.rate),
            ...     init=1.0,
            ...     into="cum_return",
            ... )
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )
        if not callable(fn):
            raise TypeError(f"fold() fn must be callable, got {type(fn).__name__}")
        if not isinstance(into, str) or not into:
            raise ValueError("fold() requires a non-empty 'into' column name")
        if into in self._schema:
            raise ValueError(
                f"fold() 'into' column '{into}' already exists in the schema"
            )
        if not self._sort_keys:
            from .exceptions import SortRequiredError

            raise SortRequiredError(
                "fold() requires a defined row order. Call .sort(...) "
                "(or .assume_sorted(...)) before fold()."
            )
        if partition_by is not None and partition_by not in self._schema:
            raise ValueError(
                f"fold() partition_by column '{partition_by}' not found in schema. "
                f"Available columns: {list(self._schema.keys())}"
            )

        from .io_ops import _infer_schema_from_rows

        import copy

        rows = self.to_dicts()

        if partition_by is None:
            state = copy.deepcopy(init)
            for row in rows:
                state = fn(state, _FoldRow(row))
                row[into] = state
        else:
            states: dict[Any, Any] = {}
            for row in rows:
                key = row.get(partition_by)
                # Each partition gets its own fresh copy of ``init`` so mutable
                # initial state (list/dict/object) is never shared across groups.
                state = states[key] if key in states else copy.deepcopy(init)
                state = fn(state, _FoldRow(row))
                states[key] = state
                row[into] = state

        # Infer the new column's type from the produced values so the schema
        # is complete; fall back to the existing schema for the other columns.
        out_schema = dict(self._schema)
        if rows:
            sample = next((r[into] for r in rows if r[into] is not None), None)
            if sample is not None:
                out_schema[into] = _infer_schema_from_rows({into: sample})[into]
            else:
                out_schema[into] = "Float64"
        else:
            out_schema[into] = "Float64"

        result = LTSeq._from_rows(rows, out_schema)
        # The accumulation preserved input order; re-declare it so downstream
        # window ops keep working without a redundant physical sort.
        sort_cols = [c for c, _ in self._sort_keys]
        sort_desc = [d for _, d in self._sort_keys]
        if sort_cols:
            return result.assume_sorted(*sort_cols, desc=sort_desc)
        return result

    def distinct(self, *key_exprs: str | Callable) -> "LTSeq":
        """
        Deduplicate rows by key columns.

        Args:
            *key_exprs: Key columns for deduplication.
                       If empty, deduplicate by all columns.

        Returns:
            Deduplicated LTSeq

        Example:
            >>> unique = t.distinct("customer_id")
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        exprs = _collect_key_exprs(key_exprs, self._schema, self._capture_expr)
        result_inner = self._inner.distinct(exprs)

        return LTSeq._from_inner(result_inner)

    def slice(self, offset: int = 0, length: int | None = None) -> "LTSeq":
        """
        Select a contiguous row range.

        Args:
            offset: Starting row (0-based)
            length: Number of rows (None means to the end)

        Returns:
            Sliced LTSeq

        Example:
            >>> t.slice(offset=10, length=5)
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        if length is not None and length < 0:
            raise ValueError(f"length must be non-negative, got {length}")

        result_inner = self._inner.slice(offset, length)

        return LTSeq._from_inner(result_inner)

    def head(self, n: int = 10) -> "LTSeq":
        """
        Return the first n rows.

        Args:
            n: Number of rows to return (default 10)

        Returns:
            LTSeq with first n rows

        Raises:
            ValueError: If n is negative

        Example:
            >>> top_10 = t.sort("score", desc=True).head(10)
        """
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
        return self.slice(offset=0, length=n)

    def tail(self, n: int = 10) -> "LTSeq":
        """
        Return the last n rows.

        Args:
            n: Number of rows to return (default 10)

        Returns:
            LTSeq with last n rows

        Raises:
            ValueError: If n is negative

        Example:
            >>> recent = t.sort("date").tail(5)
        """
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
        total = len(self)
        offset = max(0, total - n)
        return self.slice(offset=offset, length=n)

    def search_pattern(self, *step_predicates: Callable, partition_by: str | None = None) -> "LTSeq":
        """
        Find rows where consecutive rows match a sequence of predicates.

        Each step predicate is a lambda evaluated against successive rows.
        With partition_by, the pattern cannot cross partition boundaries.

        Returns the rows where step 1 matched (i.e., row i where
        step1(i), step2(i+1), ..., stepN(i+N-1) all match).

        Args:
            *step_predicates: Lambda predicates for each step in the pattern.
                             E.g., lambda r: r.url.s.starts_with("http://example.com")
            partition_by: Optional column name for partitioning. Pattern
                         matching does not cross partition boundaries.

        Returns:
            New LTSeq with matching rows

        Example:
            >>> # Find 3-step URL funnel
            >>> matches = t.search_pattern(
            ...     lambda r: r.url.s.starts_with("http://example.com/landing"),
            ...     lambda r: r.url.s.starts_with("http://example.com/product"),
            ...     lambda r: r.url.s.starts_with("http://example.com/checkout"),
            ...     partition_by="userid"
            ... )
            >>> print(matches.count())
        """
        from .core import LTSeq

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() or read_parquet() first."
            )

        if not step_predicates:
            raise ValueError("search_pattern requires at least one step predicate")

        step_dicts = [self._capture_expr(p) for p in step_predicates]
        result_inner = self._inner.search_pattern(step_dicts, partition_by)

        return LTSeq._from_inner(result_inner)

    def search_pattern_count(self, *step_predicates: Callable, partition_by: str | None = None) -> int:
        """
        Count rows where consecutive rows match a sequence of predicates.

        Optimized version of search_pattern(...).count() that avoids
        collecting the full table — only collects the columns needed
        for predicate evaluation.

        Args:
            *step_predicates: Lambda predicates for each step in the pattern.
            partition_by: Optional column name for partitioning.

        Returns:
            Number of matching pattern instances

        Example:
            >>> count = t.search_pattern_count(
            ...     lambda r: r.url.s.starts_with("http://example.com/landing"),
            ...     lambda r: r.url.s.starts_with("http://example.com/product"),
            ...     lambda r: r.url.s.starts_with("http://example.com/checkout"),
            ...     partition_by="userid"
            ... )
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() or read_parquet() first."
            )

        if not step_predicates:
            raise ValueError("search_pattern_count requires at least one step predicate")

        step_dicts = [self._capture_expr(p) for p in step_predicates]
        return self._inner.search_pattern_count(step_dicts, partition_by)
