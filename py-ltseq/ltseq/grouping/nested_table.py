"""NestedTable class for group-ordered operations."""

import re
import textwrap
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from .proxies import GroupProxy, RowProxy
from .sql_parsing import (
    DeriveSQLParser,
    FilterSQLParser,
    extract_lambda_from_chain,
    get_derive_parse_error_message,
    get_unsupported_derive_error,
    group_expr_to_sql,
)

if TYPE_CHECKING:
    from ..core import LTSeq
    from ..expr import ColumnExpr


class NestedTable:
    """
    Represents a table grouped by consecutive identical values.

    Created by LTSeq.group_ordered() or LTSeq.group_sorted(), this wrapper provides
    group-level operations like first(), last(), count() while maintaining the
    underlying row data.

    The difference between group_ordered and group_sorted is semantic:
    - group_ordered: groups consecutive identical values (no assumption about sorting)
    - group_sorted: assumes data is globally sorted by key (consecutive = all same key)

    Both use the same underlying algorithm (consecutive grouping), but group_sorted
    documents the expectation that data is pre-sorted.
    """

    def __init__(
        self,
        ltseq_instance: "LTSeq",
        grouping_lambda: Callable,
        is_sorted: bool = False,
    ):
        """
        Initialize a NestedTable from an LTSeq and grouping function.

        Args:
            ltseq_instance: The LTSeq table to group
            grouping_lambda: Lambda that returns the grouping key for each row
            is_sorted: If True, indicates the data is globally sorted by the key
                      (from group_sorted). If False, groups only consecutive
                      identical values (from group_ordered).
        """
        self._ltseq = ltseq_instance
        self._grouping_lambda = grouping_lambda
        self._schema = ltseq_instance._schema.copy()
        self._is_sorted = is_sorted

        # Add internal group columns to schema
        self._schema["__group_id__"] = "int64"
        self._schema["__group_count__"] = "int64"
        self._schema["__rn__"] = "int64"

        # Optional filters and derivations
        self._group_filter = None
        self._group_derive = None

        # Store group assignments for rows (used when filter() is applied)
        self._group_assignments = None

        # SQL parsers
        self._filter_parser = FilterSQLParser()
        self._derive_parser = DeriveSQLParser()

    def __len__(self) -> int:
        """Return the number of rows in the grouped table."""
        return len(self._ltseq.to_pandas())

    def to_pandas(self):
        """Convert the grouped table to a pandas DataFrame."""
        return self._ltseq.to_pandas()

    def first(self) -> "LTSeq":
        """
        Get the first row of each group.

        Returns an LTSeq containing only the first row within each group.
        The returned LTSeq has a fast path for .count() that avoids
        materializing per-row metadata arrays.

        Example:
            >>> grouped.first()  # Returns LTSeq with first row per group
            >>> grouped.first().count()  # Fast path: just counts groups
        """
        result = _LazyFirstLTSeq(self)
        return result

    def last(self) -> "LTSeq":
        """
        Get the last row of each group.

        Returns an LTSeq containing only the last row within each group.

        Example:
            >>> grouped.last()  # Returns LTSeq with last row per group
        """
        flattened = self.flatten()
        result = flattened.__class__()
        result._schema = flattened._schema.copy()
        result._inner = flattened._inner.last_row()
        return result

    def count(self) -> "ColumnExpr":
        """
        Get the count of rows in each group.

        Returns a ColumnExpr representing the row count for each group.

        Example:
            >>> grouped.count()  # Number of rows in each group
        """
        from ..expr import CallExpr

        return CallExpr("__count__", (), {}, on=None)

    def flatten(self) -> "LTSeq":
        """
        Return the underlying LTSeq with __group_id__ column added.

        The __group_id__ column identifies consecutive groups with identical
        grouping values.

        Returns:
            A new LTSeq with __group_id__ column for each row

        Example:
            >>> grouped = t.group_ordered(lambda r: r.is_up)
            >>> flattened = grouped.flatten()  # Now has __group_id__ column
        """
        expr_dict = self._ltseq._capture_expr(self._grouping_lambda)

        result = self._ltseq.__class__()
        result._schema = self._schema.copy()
        result._inner = self._ltseq._inner.group_id(expr_dict)

        return result

    def filter(self, group_predicate: Callable) -> "NestedTable":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last(), g.max('col'), etc.

        Returns:
            A NestedTable with only rows from groups that pass the predicate

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
            >>> grouped.filter(lambda g: g.first().price > 100)
            >>> grouped.filter(lambda g: g.all(lambda r: r.amount > 0))
        """
        # Try SQL-based approach first
        try:
            where_clause = self._filter_parser.try_parse_filter_to_sql(group_predicate)
            if where_clause:
                return self._filter_via_sql(where_clause)
        except Exception:
            pass

        # Fall back to pandas-based evaluation
        return self._filter_via_pandas(group_predicate)

    def _filter_via_sql(self, where_clause: str) -> "NestedTable":
        """Filter using SQL WHERE clause with window functions."""
        flattened = self.flatten()
        filtered_inner = flattened._inner.filter_where(where_clause)

        result_ltseq = self._ltseq.__class__()
        result_ltseq._inner = filtered_inner
        result_ltseq._schema = flattened._schema.copy()

        result_nested = NestedTable(
            result_ltseq, self._grouping_lambda, is_sorted=self._is_sorted
        )
        return result_nested

    def _filter_via_pandas(self, group_predicate: Callable) -> "NestedTable":
        """Filter using pandas iteration (fallback for complex predicates)."""
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "filter() requires pandas. Install it with: pip install pandas"
            )

        df = self._ltseq.to_pandas()

        # Add __group_id__ column
        group_values = []
        group_id = 0
        prev_group_val = None

        for idx, row in df.iterrows():
            row_proxy = RowProxy(row.to_dict())
            group_val = self._grouping_lambda(row_proxy)

            if prev_group_val is None or group_val != prev_group_val:
                group_id += 1
                prev_group_val = group_val

            group_values.append(group_id)

        df["__group_id__"] = group_values
        df["__group_count__"] = df.groupby("__group_id__").transform("size")

        # Evaluate predicate for each group
        group_by_id = df.groupby("__group_id__")
        passing_group_ids = set()

        for gid, group_df in group_by_id:
            group_proxy = GroupProxy(group_df, self)

            try:
                result = group_predicate(group_proxy)
                if result:
                    passing_group_ids.add(gid)
            except AttributeError as e:
                supported_methods = """
Supported group methods:
- g.count() - number of rows in group
- g.first().column - first row's column value
- g.last().column - last row's column value
- g.max('column') - maximum value in column
- g.min('column') - minimum value in column
- g.sum('column') - sum of column values
- g.avg('column') - average of column values
"""
                raise ValueError(
                    f"Error evaluating filter predicate on group {gid}: {e}\n{supported_methods}"
                )
            except Exception as e:
                raise ValueError(
                    f"Error evaluating filter predicate on group {gid}: {e}"
                )

        # Filter to keep only passing groups
        filtered_df = df[df["__group_id__"].isin(list(passing_group_ids))]

        # Store group assignments
        group_assignments = {}
        for original_idx, (_, row) in enumerate(filtered_df.iterrows()):
            group_assignments[original_idx] = int(row["__group_id__"])

        # Remove internal columns
        result_cols = [c for c in filtered_df.columns if not c.startswith("__")]
        result_df = filtered_df[result_cols]

        # Convert back to LTSeq
        rows = result_df.to_dict("records")
        schema = self._ltseq._schema.copy()

        result_ltseq = self._ltseq.__class__._from_rows(rows, schema)

        result_nested = NestedTable(
            result_ltseq, self._grouping_lambda, is_sorted=self._is_sorted
        )
        result_nested._group_assignments = group_assignments
        return result_nested

    def derive(self, group_mapper: Callable) -> "LTSeq":
        """
        Derive new columns based on group properties.

        Each group property (first row, last row, count, etc.) is broadcasted
        to all rows in that group.

        Args:
            group_mapper: Lambda taking a group proxy (g) and returning a dict
                         of new columns.

        Returns:
            LTSeq with original rows plus new derived columns

        Example:
            >>> grouped.derive(lambda g: {"span": g.count()})
            >>> grouped.derive(lambda g: {
            ...     "start": g.first().date,
            ...     "end": g.last().date,
            ... })
        """
        # Use pandas when we have stored group assignments from filter()
        if self._group_assignments is not None:
            return self._derive_via_pandas_with_stored_groups(group_mapper)

        flattened = self.flatten()

        # Try proxy-based expression capture first (works in all contexts)
        try:
            derive_exprs = self._capture_derive_via_proxy(group_mapper)
            if derive_exprs:
                return self._derive_via_sql(flattened, derive_exprs)
        except Exception:
            pass  # Fall through to source parsing

        # Fall back to source code parsing (for backward compatibility)
        import ast
        import inspect

        source = None
        try:
            try:
                source = inspect.getsource(group_mapper)
            except (OSError, TypeError):
                source = None

            if source:
                source_dedented = textwrap.dedent(source)
                source_to_parse = extract_lambda_from_chain(source_dedented)

                try:
                    tree = ast.parse(source_to_parse)
                except SyntaxError:
                    try:
                        tree = ast.parse(f"({source_to_parse})", mode="eval")
                    except SyntaxError:
                        raise ValueError(
                            f"Cannot parse derive lambda expression. "
                            f"Source: {source_to_parse}"
                        )

                derive_exprs = self._derive_parser.extract_derive_expressions(
                    tree, flattened._schema
                )

                if derive_exprs:
                    return self._derive_via_sql(flattened, derive_exprs)
                else:
                    raise ValueError(get_unsupported_derive_error(source))
            else:
                raise ValueError(
                    "Cannot parse derive expression (source not available). "
                    "This can happen with lambdas defined in REPL. "
                    "Please define the lambda in a file for now."
                )
        except ValueError:
            raise
        except Exception as e:
            source_str = source if source else "<unavailable>"
            raise ValueError(get_derive_parse_error_message(source_str, str(e)))

    def _capture_derive_via_proxy(self, group_mapper: Callable) -> Dict[str, str]:
        """
        Capture derive expressions using proxy pattern.

        This approach works in all contexts (pytest, REPL, exec) because it
        doesn't rely on source code inspection.

        Args:
            group_mapper: Lambda taking a group proxy and returning dict of expressions

        Returns:
            Dict mapping column name to SQL window function expression

        Raises:
            ValueError: If the lambda doesn't return a dict of GroupExpr objects
        """
        from .proxies.derive_proxy import DeriveGroupProxy
        from .expr import GroupExpr

        proxy = DeriveGroupProxy()
        result = group_mapper(proxy)

        if not isinstance(result, dict):
            raise ValueError("Derive lambda must return a dict")

        derive_exprs = {}
        for col_name, expr in result.items():
            if not isinstance(col_name, str):
                raise ValueError(f"Column name must be a string, got {type(col_name)}")

            if isinstance(expr, GroupExpr):
                derive_exprs[col_name] = group_expr_to_sql(expr.serialize())
            else:
                raise ValueError(
                    f"Unsupported expression type for column '{col_name}': {type(expr)}. "
                    f"Expected GroupExpr (from g.count(), g.first().col, etc.)"
                )

        return derive_exprs

    def _derive_via_sql(
        self, flattened: "LTSeq", derive_exprs: Dict[str, str]
    ) -> "LTSeq":
        """Apply derived column expressions via SQL SELECT with window functions."""
        result = flattened.__class__()

        result._schema = {}
        for col_name, col_type in self._ltseq._schema.items():
            if col_name not in ("__group_id__", "__rn__", "__group_count__"):
                result._schema[col_name] = col_type
        for col_name in derive_exprs.keys():
            result._schema[col_name] = "Unknown"

        result._inner = flattened._inner.derive_window_sql(derive_exprs)

        return result

    def _derive_via_pandas_with_stored_groups(self, group_mapper: Callable) -> "LTSeq":
        """Derive columns using pandas when we have stored group assignments."""
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "derive() with stored group assignments requires pandas. "
                "Install it with: pip install pandas"
            )

        import ast
        import inspect

        try:
            source = inspect.getsource(group_mapper)
        except (OSError, TypeError):
            raise ValueError(
                "Cannot parse derive expression (source not available). "
                "This can happen with lambdas defined in REPL. "
                "Please define the lambda in a file for now."
            )

        source_dedented = textwrap.dedent(source)
        source_to_parse = extract_lambda_from_chain(source_dedented)

        try:
            tree = ast.parse(source_to_parse)
        except SyntaxError:
            try:
                tree = ast.parse(f"({source_to_parse})", mode="eval")
            except SyntaxError:
                raise ValueError(
                    f"Cannot parse derive lambda expression. Source: {source_to_parse}"
                )

        schema = self._ltseq._schema.copy()
        derive_exprs = self._derive_parser.extract_derive_expressions(tree, schema)

        if not derive_exprs:
            raise ValueError(get_unsupported_derive_error(source))

        df = self._ltseq.to_pandas()

        df["__rn__"] = range(len(df))
        group_values = [self._group_assignments[i] for i in range(len(df))]
        df["__group_id__"] = group_values

        for col_name, sql_expr in derive_exprs.items():
            try:
                df[col_name] = self._evaluate_sql_expr_in_pandas(df, sql_expr)
            except Exception as e:
                raise ValueError(f"Error evaluating derived column '{col_name}': {e}")

        df = df.drop(columns=["__group_id__", "__rn__"])

        result_schema = self._ltseq._schema.copy()
        for col_name in derive_exprs.keys():
            result_schema[col_name] = "Unknown"

        rows = df.to_dict("records")
        result = self._ltseq.__class__._from_rows(rows, result_schema)

        return result

    def _evaluate_sql_expr_in_pandas(self, df, sql_expr: str):
        """Evaluate SQL-like window function expression in pandas."""
        # Handle arithmetic expressions recursively
        if sql_expr.startswith("(") and sql_expr.endswith(")"):
            inner = sql_expr[1:-1]

            paren_depth = 0
            for i, char in enumerate(inner):
                if char == "(":
                    paren_depth += 1
                elif char == ")":
                    paren_depth -= 1
                elif paren_depth == 0 and char in ["+", "-", "*", "/"]:
                    left_expr = inner[:i].strip()
                    op = char
                    right_expr = inner[i + 1 :].strip()

                    left_result = self._evaluate_sql_expr_in_pandas(df, left_expr)
                    right_result = self._evaluate_sql_expr_in_pandas(df, right_expr)

                    if op == "+":
                        return left_result + right_result
                    elif op == "-":
                        return left_result - right_result
                    elif op == "*":
                        return left_result * right_result
                    elif op == "/":
                        return left_result / right_result

        # Handle COUNT(*)
        if "COUNT(*)" in sql_expr and "PARTITION BY __group_id__" in sql_expr:
            return df.groupby("__group_id__").transform("size")

        # Handle FIRST_VALUE(col)
        if "FIRST_VALUE(" in sql_expr:
            match = re.search(r"FIRST_VALUE\((\w+)\)", sql_expr)
            if match:
                col_name = match.group(1)
                return df.groupby("__group_id__")[col_name].transform("first")

        # Handle LAST_VALUE(col)
        if "LAST_VALUE(" in sql_expr:
            match = re.search(r"LAST_VALUE\((\w+)\)", sql_expr)
            if match:
                col_name = match.group(1)
                return df.groupby("__group_id__")[col_name].transform("last")

        # Handle MAX/MIN/SUM/AVG
        for agg_sql, agg_pandas in [
            ("MAX", "max"),
            ("MIN", "min"),
            ("SUM", "sum"),
            ("AVG", "mean"),
        ]:
            if f"{agg_sql}(" in sql_expr:
                match = re.search(f"{agg_sql}\\((\\w+)\\)", sql_expr)
                if match:
                    col_name = match.group(1)
                    return df.groupby("__group_id__")[col_name].transform(agg_pandas)

        return df.eval(sql_expr)


class _LazyFirstLTSeq:
    """
    Lazy wrapper returned by NestedTable.first().

    Defers materialization until needed. Intercepts count()/len() to use the
    fast group_ordered_count() Rust path that avoids building per-row metadata
    arrays (saves ~0.71s on 100M rows).

    For all other operations, materializes eagerly via flatten().first_row()
    and delegates to the resulting LTSeq.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    def __init__(self, nested: "NestedTable"):
        # Intentionally do NOT call LTSeq.__init__ — we are lazy.
        self._nested = nested
        self._materialized = None
        # Set LTSeq-expected attributes to avoid AttributeError before
        # __getattr__ kicks in (these are checked by some LTSeq methods).
        self._schema = nested._ltseq._schema.copy()
        self._sort_keys = nested._ltseq._sort_keys
        self._name = None

    def _materialize(self):
        """Eagerly materialize the first-row-per-group LTSeq."""
        if self._materialized is None:
            flattened = self._nested.flatten()
            from ..core import LTSeq
            result = LTSeq()
            result._schema = flattened._schema.copy()
            result._inner = flattened._inner.first_row()
            self._materialized = result
        return self._materialized

    # -- Fast path: count / len --

    def count(self) -> int:
        """Fast path: count groups without materializing per-row arrays."""
        nested = self._nested
        ltseq = nested._ltseq
        expr_dict = ltseq._capture_expr(nested._grouping_lambda)
        try:
            return ltseq._inner.group_ordered_count(expr_dict)
        except Exception:
            # Fallback: materialize and count
            return self._materialize().count()

    def __len__(self) -> int:
        return self.count()

    # -- Delegate everything else to materialized LTSeq --

    def __getattr__(self, name):
        """Delegate attribute access to the materialized LTSeq."""
        return getattr(self._materialize(), name)
