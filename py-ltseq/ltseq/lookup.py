"""Lookup mixin for handling lookup expressions in derive operations."""

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from .core import LTSeq


class LookupMixin:
    """Mixin providing lookup resolution for derive operations."""

    # These will be provided by the main class
    _schema: Dict[str, str]
    _inner: Any
    _sort_keys: Any

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
