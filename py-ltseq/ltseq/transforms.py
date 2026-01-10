"""Transform operations for LTSeq: filter, select, derive, sort, distinct, slice."""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .expr import SchemaProxy


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


class TransformMixin:
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

        # Check for lookup expressions
        has_lookup = any(self._contains_lookup(expr) for expr in derived_cols.values())

        if has_lookup:
            # Resolve lookups by performing joins
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

    def _contains_lookup(self, expr_dict: Dict[str, Any]) -> bool:
        """
        Recursively check if expression contains a Lookup.

        Args:
            expr_dict: Serialized expression dict

        Returns:
            True if expression contains a Lookup type
        """
        if not isinstance(expr_dict, dict):
            return False
        if expr_dict.get("type") == "Lookup":
            return True
        # Check nested expressions
        for key in ["on", "left", "right", "operand"]:
            if key in expr_dict and self._contains_lookup(expr_dict[key]):
                return True
        # Check args list
        for arg in expr_dict.get("args", []):
            if self._contains_lookup(arg):
                return True
        # Check kwargs values
        for kwarg_val in expr_dict.get("kwargs", {}).values():
            if self._contains_lookup(kwarg_val):
                return True
        return False

    def _extract_lookup_info(
        self, expr_dict: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract lookup information from an expression.

        Args:
            expr_dict: Serialized expression dict

        Returns:
            Dict with lookup info, or None if not a Lookup
        """
        if not isinstance(expr_dict, dict):
            return None

        if expr_dict.get("type") == "Lookup":
            # Extract the source column from the 'on' expression
            on_expr = expr_dict.get("on", {})
            on_column = None

            if on_expr.get("type") == "Column":
                on_column = on_expr.get("name")
            # For chained calls like r.product_id.lower().lookup(...)
            # we need to find the root column
            elif on_expr.get("type") == "Call":
                inner = on_expr.get("on", {})
                while inner.get("type") == "Call":
                    inner = inner.get("on", {})
                if inner.get("type") == "Column":
                    on_column = inner.get("name")

            return {
                "on_column": on_column,
                "on_expr": on_expr,
                "target_name": expr_dict.get("target_name"),
                "target_columns": expr_dict.get("target_columns", []),
                "join_key": expr_dict.get("join_key"),
            }

        # Recursively check nested expressions
        for key in ["on", "left", "right", "operand"]:
            if key in expr_dict:
                nested = self._extract_lookup_info(expr_dict[key])
                if nested:
                    return nested

        for arg in expr_dict.get("args", []):
            nested = self._extract_lookup_info(arg)
            if nested:
                return nested

        return None

    def _resolve_lookups(
        self, derived_cols: Dict[str, Dict]
    ) -> Tuple["LTSeq", Dict[str, Dict]]:
        """
        Resolve lookup expressions by joining with target tables.

        This method transforms lookup expressions into actual joins,
        then rewrites the expressions to reference the joined columns.

        Args:
            derived_cols: Dict mapping column names to serialized expressions

        Returns:
            Tuple of (joined_table, simplified_derived_cols)
            - joined_table: LTSeq with target tables joined in
            - simplified_derived_cols: Expressions rewritten to use joined columns
        """
        from .core import LTSeq
        from .expr.types import LookupExpr

        current = self
        simplified = {}
        joined_targets = {}  # Track which targets we've already joined

        for col_name, expr_dict in derived_cols.items():
            if not self._contains_lookup(expr_dict):
                # No lookup in this expression, keep as-is
                simplified[col_name] = expr_dict
                continue

            # Extract lookup info
            lookup_info = self._extract_lookup_info(expr_dict)
            if lookup_info is None:
                simplified[col_name] = expr_dict
                continue

            target_name = lookup_info["target_name"]
            target_table = LookupExpr.get_table(target_name)

            if target_table is None:
                raise ValueError(
                    f"Lookup target table '{target_name}' not found. "
                    "Make sure the table is passed to lookup()."
                )

            # Generate alias for joined columns
            # Rust join creates columns as {alias}_{column}, so use simpler alias
            alias = f"_lkp_{target_name}"

            # Only join each target once
            if target_name not in joined_targets:
                # Determine join keys
                on_column = lookup_info["on_column"]
                join_key = lookup_info["join_key"]

                if on_column is None:
                    raise ValueError(
                        f"Could not determine source column for lookup on '{target_name}'. "
                        "The lookup must be called on a column expression."
                    )

                # If no explicit join_key, use the first target column
                if join_key is None:
                    # Default: assume we're joining on a column with same name
                    # or the first target column
                    target_schema = getattr(target_table, "_schema", {})
                    if on_column in target_schema:
                        join_key = on_column
                    else:
                        # Try first column of target as join key
                        target_cols = list(target_schema.keys())
                        if target_cols:
                            join_key = target_cols[0]
                        else:
                            raise ValueError(
                                f"Cannot determine join key for lookup on '{target_name}'. "
                                f"Please specify join_key explicitly."
                            )

                # Perform left join to bring in lookup columns
                # Use the existing join infrastructure
                current = current._perform_lookup_join(
                    target_table, on_column, join_key, alias
                )
                joined_targets[target_name] = alias

            # Rewrite expression to reference joined column
            alias = joined_targets[target_name]
            for target_col in lookup_info["target_columns"]:
                # Rust join creates columns as {alias}_{column}
                joined_col_name = f"{alias}_{target_col}"
                simplified[col_name] = {"type": "Column", "name": joined_col_name}

        return current, simplified

    def _perform_lookup_join(
        self, target_table: "LTSeq", on_column: str, join_key: str, alias: str
    ) -> "LTSeq":
        """
        Perform a left join with a target table for lookup resolution.

        Args:
            target_table: The table to join with
            on_column: Column name in self to join on
            join_key: Column name in target_table to join on
            alias: Prefix for target table columns

        Returns:
            New LTSeq with joined data
        """
        from .core import LTSeq

        # Use Rust join implementation
        left_key_expr = {"type": "Column", "name": on_column}
        right_key_expr = {"type": "Column", "name": join_key}

        result_inner = self._inner.join(
            target_table._inner,
            left_key_expr,
            right_key_expr,
            "left",  # Use left join so unmatched rows get NULL
            alias,
        )

        result = LTSeq()
        result._inner = result_inner

        # Combine schemas
        # Rust join creates columns as {alias}_{column}
        result._schema = self._schema.copy()
        target_schema = getattr(target_table, "_schema", {})
        for col_name, col_type in target_schema.items():
            result._schema[f"{alias}_{col_name}"] = col_type

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
