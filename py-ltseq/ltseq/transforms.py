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
                        simplified_cols
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
                derived_cols
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

    # Alias for discoverability (Polars users expect with_columns)
    with_columns = derive

    def rename(self, mapping: Dict[str, str] = None, **kwargs: str) -> "LTSeq":
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

        # Build select expressions with aliases
        select_exprs = []
        for col_name in self._schema:
            if col_name in renames:
                new_name = renames[col_name]
                select_exprs.append({
                    "type": "Alias",
                    "expr": {"type": "Column", "name": col_name},
                    "alias": new_name,
                })
            else:
                select_exprs.append({"type": "Column", "name": col_name})

        result_inner = self._inner.select(select_exprs)

        result = LTSeq()
        result._inner = result_inner

        # Build new schema with renamed columns (preserving order)
        new_schema = {}
        for col_name, col_type in self._schema.items():
            new_name = renames.get(col_name, col_name)
            new_schema[new_name] = col_type
        result._schema = new_schema

        # Update sort keys if any were renamed
        if self._sort_keys:
            result._sort_keys = [
                (renames.get(col, col), is_desc)
                for col, is_desc in self._sort_keys
            ]
        else:
            result._sort_keys = None

        return result

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

        result = LTSeq()
        result._inner = result_inner
        result._schema = {
            col: self._schema[col] for col in remaining_cols
        }

        # Update sort keys — remove any that were dropped
        if self._sort_keys:
            new_sort_keys = [
                (col, is_desc)
                for col, is_desc in self._sort_keys
                if col not in cols_to_drop
            ]
            result._sort_keys = new_sort_keys if new_sort_keys else None
        else:
            result._sort_keys = None

        return result

    def _has_window_functions(self, derived_cols: Dict[str, Any]) -> bool:
        """Check if derived columns contain window functions."""

        def check_expr(expr: Dict[str, Any]) -> bool:
            if not isinstance(expr, dict):
                return False
            expr_type = expr.get("type", "")
            if expr_type == "Window":
                return True
            if expr_type == "Call":
                func = expr.get("func", "")
                if func in ("shift", "diff", "rolling", "cum_sum"):
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

    def assume_sorted(
        self,
        *keys: str,
        desc: Union[bool, List[bool]] = False,
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
        result_inner = self._inner.assume_sorted(key_exprs)

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()

        # Track sort keys
        sort_keys = [(key, desc_list[i]) for i, key in enumerate(keys)]
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

    def search_pattern(self, *step_predicates: Callable, partition_by: Optional[str] = None) -> "LTSeq":
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

        result = LTSeq()
        result._inner = result_inner
        result._schema = self._schema.copy()
        result._sort_keys = self._sort_keys
        return result

    def search_pattern_count(self, *step_predicates: Callable, partition_by: Optional[str] = None) -> int:
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
