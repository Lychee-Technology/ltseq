"""Transform operations for LTSeq: filter, select, derive, sort, distinct, slice."""

from typing import Any, Callable, Dict, List, Optional, Union

from .expr import SchemaProxy
from .lookup import LookupMixin


def _process_select_col(col, schema: Dict[str, str]):
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


def _extract_derive_cols(args, kwargs, schema, capture_expr_fn):
    """Extract derived columns from derive() arguments."""
    if args:
        if len(args) > 1:
            raise TypeError(
                f"derive() takes at most 1 positional argument ({len(args)} given)"
            )
        if kwargs:
            raise TypeError(
                "derive() cannot use both positional argument and keyword arguments"
            )

        func = args[0]
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


def _collect_key_exprs(key_exprs, schema, capture_expr_fn):
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


class TransformMixin(LookupMixin):
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

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = self._sort_keys
        return result

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

        result = LTSeq()
        result._inner = result_inner
        # Keep full schema (proper schema reduction requires Rust support)
        result._schema = self._schema.copy()

        # Clear sort keys if any sort column is not in selected columns
        if self._sort_keys:
            sort_cols_present = all(col in selected_cols for col, _ in self._sort_keys)
            result._sort_keys = self._sort_keys if sort_cols_present else None
        else:
            result._sort_keys = None
        return result

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
                if has_window and resolved_table._sort_keys:
                    result_inner = resolved_table._inner.derive_with_window_functions(
                        simplified_cols, resolved_table._sort_keys
                    )
                else:
                    result_inner = resolved_table._inner.derive(simplified_cols)

                result = LTSeq()
                result._inner = result_inner
                result._schema = resolved_table._schema.copy()
                for col_name in simplified_cols.keys():
                    result._schema[col_name] = "Unknown"
                result._sort_keys = resolved_table._sort_keys
                return result
            finally:
                # Always clear the registry after derive completes
                LookupExpr.clear_registry()

        # Normal derive path (no lookups)
        has_window = self._has_window_functions(derived_cols)

        if has_window and self._sort_keys:
            result_inner = self._inner.derive_with_window_functions(
                derived_cols, self._sort_keys
            )
        else:
            result_inner = self._inner.derive(derived_cols)

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        for col_name in derived_cols.keys():
            result._schema[col_name] = "Unknown"
        result._sort_keys = self._sort_keys
        return result

    def _has_window_functions(self, derived_cols: Dict[str, Any]) -> bool:
        """Check if derived columns contain window functions."""

        def check_expr(expr: Dict[str, Any]) -> bool:
            if not isinstance(expr, dict):
                return False
            expr_type = expr.get("type", "")
            if expr_type == "Call":
                method = expr.get("method", "")
                if method in ("shift", "diff", "rolling"):
                    return True
                for arg in expr.get("args", []):
                    if check_expr(arg):
                        return True
            elif expr_type == "BinOp":
                if check_expr(expr.get("left", {})):
                    return True
                if check_expr(expr.get("right", {})):
                    return True
            return False

        for expr in derived_cols.values():
            if check_expr(expr):
                return True
        return False

    def sort(
        self,
        *keys: Union[str, Callable],
        desc: Union[bool, List[bool]] = False,
        descending: Union[bool, List[bool]] = None,
    ) -> "LTSeq":
        """
        Sort by one or more keys.

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

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()

        # Track sort keys
        sort_keys = []
        for i, key in enumerate(keys):
            if isinstance(key, str):
                sort_keys.append((key, desc_list[i]))
            elif callable(key):
                expr = key_exprs[i]
                if expr.get("type") == "Column":
                    sort_keys.append((expr["name"], desc_list[i]))
        result._sort_keys = sort_keys if sort_keys else None

        return result

    def distinct(self, *key_exprs: Union[str, Callable]) -> "LTSeq":
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

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = None
        return result

    def slice(self, offset: int = 0, length: Optional[int] = None) -> "LTSeq":
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

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = self._sort_keys
        return result
