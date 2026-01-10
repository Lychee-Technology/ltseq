"""NestedTable class for group-ordered operations."""

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    from .core import LTSeq
    from .expr import ColumnExpr


class GroupProxy:
    """
    Proxy for evaluating predicates on a group during filter operations.

    Provides access to group properties like count(), first(), last()
    so that predicates can be evaluated.
    """

    def __init__(self, group_data, nested_table):
        """
        Initialize a group proxy.

        Args:
            group_data: DataFrame containing rows for this group (list of dicts or pandas DataFrame)
            nested_table: The NestedTable this group belongs to
        """
        self._group_data = group_data
        self._nested_table = nested_table

    def count(self):
        """Get the number of rows in this group."""
        if isinstance(self._group_data, list):
            return len(self._group_data)
        else:
            return len(self._group_data)

    def first(self):
        """Get the first row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            first_row_data = self._group_data[0]
        else:
            first_row_data = self._group_data.iloc[0]
        return RowProxy(first_row_data)

    def last(self):
        """Get the last row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            last_row_data = self._group_data[-1]
        else:
            last_row_data = self._group_data.iloc[-1]
        return RowProxy(last_row_data)

    def max(self, column_name=None):
        """
        Get the maximum value in a column for this group.

        Args:
            column_name: Name of the column to find max for.
                        If not provided, returns self for chaining.

        Returns:
            The maximum value in the column for this group.

        Example:
            >>> g.max('price')  # Max price in group
        """
        if column_name is None:
            # Return self to support chaining
            return self

        # Find max value in the specified column
        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        # Filter out None values and return max
        valid_values = [v for v in values if v is not None]
        return max(valid_values) if valid_values else None

    def min(self, column_name=None):
        """
        Get the minimum value in a column for this group.

        Args:
            column_name: Name of the column to find min for.
                        If not provided, returns self for chaining.

        Returns:
            The minimum value in the column for this group.

        Example:
            >>> g.min('price')  # Min price in group
        """
        if column_name is None:
            # Return self to support chaining
            return self

        # Find min value in the specified column
        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        # Filter out None values and return min
        valid_values = [v for v in values if v is not None]
        return min(valid_values) if valid_values else None

    def sum(self, column_name=None):
        """
        Get the sum of values in a column for this group.

        Args:
            column_name: Name of the column to sum.
                        If not provided, returns self for chaining.

        Returns:
            The sum of values in the column for this group.

        Example:
            >>> g.sum('quantity')  # Total quantity in group
        """
        if column_name is None:
            # Return self to support chaining
            return self

        # Sum values in the specified column
        if isinstance(self._group_data, list):
            values = [
                row.get(column_name, 0) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        # Filter out None values and sum
        valid_values = [v for v in values if v is not None]
        return sum(valid_values) if valid_values else 0

    def avg(self, column_name=None):
        """
        Get the average value in a column for this group.

        Args:
            column_name: Name of the column to average.
                        If not provided, returns self for chaining.

        Returns:
            The average value in the column for this group.

        Example:
            >>> g.avg('price')  # Average price in group
        """
        if column_name is None:
            # Return self to support chaining
            return self

        # Average values in the specified column
        if isinstance(self._group_data, list):
            values = [
                row.get(column_name) if isinstance(row, dict) else row[column_name]
                for row in self._group_data
            ]
        else:
            values = self._group_data[column_name].values

        # Filter out None values and compute average
        valid_values = [v for v in values if v is not None]
        if valid_values:
            return sum(valid_values) / len(valid_values)
        return None


class RowProxy:
    """Proxy for accessing columns of a row during predicate evaluation."""

    def __init__(self, row_data):
        """
        Initialize a row proxy.

        Args:
            row_data: A pandas Series, dict, or row-like object
        """
        self._row_data = row_data

    def __getattr__(self, col_name: str):
        """Access a column value from the row."""
        if col_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{col_name}'"
            )

        # Handle dict-like access
        if isinstance(self._row_data, dict):
            if col_name in self._row_data:
                return self._row_data[col_name]
        # Handle pandas Series access
        elif hasattr(self._row_data, "__getitem__"):
            try:
                return self._row_data[col_name]
            except (KeyError, IndexError):
                pass

        raise AttributeError(f"Column '{col_name}' not found in row")


class NestedTable:
    """
    Represents a table grouped by consecutive identical values.

    Created by LTSeq.group_ordered(), this wrapper provides group-level operations
    like first(), last(), count() while maintaining the underlying row data.
    """

    def __init__(self, ltseq_instance: "LTSeq", grouping_lambda: Callable):
        """
        Initialize a NestedTable from an LTSeq and grouping function.

        Args:
            ltseq_instance: The LTSeq table to group
            grouping_lambda: Lambda that returns the grouping key for each row
        """
        self._ltseq = ltseq_instance
        self._grouping_lambda = grouping_lambda
        self._schema = ltseq_instance._schema.copy()

        # Add internal group columns to schema
        self._schema["__group_id__"] = "int64"
        self._schema["__group_count__"] = "int64"
        self._schema["__rn__"] = "int64"  # Row number within each group

        # Optional filters and derivations (set later if needed)
        self._group_filter = None
        self._group_derive = None

        # Store group assignments for rows (used when filter() is applied)
        # Maps row_index -> group_id
        self._group_assignments = None

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

        Example:
            >>> grouped.first()  # Returns LTSeq with first row per group
            >>> grouped.first().quantity  # Column access (future enhancement)
        """
        # Materialize the grouping to add __group_id__
        flattened = self.flatten()

        # Call Rust method to filter to first row per group
        result = flattened.__class__()
        result._schema = flattened._schema.copy()
        result._inner = flattened._inner.first_row()

        return result

    def last(self) -> "LTSeq":
        """
        Get the last row of each group.

        Returns an LTSeq containing only the last row within each group.

        Example:
            >>> grouped.last()  # Returns LTSeq with last row per group
            >>> grouped.last().quantity  # Column access (future enhancement)
        """
        # Materialize the grouping to add __group_id__
        flattened = self.flatten()

        # Call Rust method to filter to last row per group
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
        from .expr import CallExpr

        return CallExpr("__count__", (), {}, on=None)

    def flatten(self) -> "LTSeq":
        """
        Return the underlying LTSeq with __group_id__ column added.

        The __group_id__ column identifies consecutive groups with identical
        grouping values. For example, if grouping by is_up, all consecutive
        rows with is_up=1 get the same __group_id__.

        Phase B2: Materializes the lazy grouping by computing and adding the
        __group_id__ column via the consecutive identical grouping algorithm.

        Returns:
            A new LTSeq with __group_id__ column for each row

        Example:
            >>> grouped = t.group_ordered(lambda r: r.is_up)
            >>> flattened = grouped.flatten()  # Now has __group_id__ column
        """
        # Capture the grouping expression
        expr_dict = self._ltseq._capture_expr(self._grouping_lambda)

        # Create a new LTSeq with group_id computed
        result = self._ltseq.__class__()
        result._schema = self._schema.copy()  # Already includes __group_id__

        # Call Rust to compute __group_id__ column
        result._inner = self._ltseq._inner.group_id(expr_dict)

        return result

    def _extract_lambda_from_chain(self, source: str) -> str:
        """
        Extract just the lambda from a source that might include method chains.

        When inspect.getsource() is called on a lambda in a chained method call like
        .filter(...).derive(lambda g: {...}), it returns the whole chain.
        This extracts just the lambda part.

        Example:
            Input: '.derive(lambda g: {"total": g.sum("val")})'
            Output: 'lambda g: {"total": g.sum("val")}'
        """
        import ast

        # Find where 'lambda' starts in the source
        lambda_pos = source.find("lambda")
        if lambda_pos == -1:
            # No lambda found, return original source
            return source

        # Extract from 'lambda' onwards
        lambda_source = source[lambda_pos:].strip()

        # Try to find the end of the lambda by parsing
        # Start with the full string and progressively trim from the end
        for end_idx in range(len(lambda_source), 0, -1):
            candidate = lambda_source[:end_idx]
            try:
                ast.parse(candidate)
                # Successfully parsed, this is the lambda
                return candidate
            except SyntaxError:
                # Try removing trailing closing parens/brackets
                if candidate and candidate[-1] in ")]}":
                    continue
                # Otherwise keep trying

        # If we can't find a valid lambda, return the extracted part without trailing chars
        # Remove any trailing closing parens, brackets, or commas
        while lambda_source and lambda_source[-1] in "),]}":
            lambda_source = lambda_source[:-1]

        return lambda_source.strip()

    def _remove_comments_from_source(self, source: str) -> str:
        """
        Remove Python comments from source code while preserving string literals.

        This is important for multi-line lambdas with inline comments,
        which would otherwise break AST parsing.
        """
        import re

        lines = source.split("\n")
        result_lines = []

        for line in lines:
            # Remove comments that are not inside strings
            # Simple approach: find # not inside quotes
            in_string = False
            quote_char = None
            result = []

            i = 0
            while i < len(line):
                char = line[i]

                # Track string state
                if char in ('"', "'") and (i == 0 or line[i - 1] != "\\"):
                    if not in_string:
                        in_string = True
                        quote_char = char
                    elif char == quote_char:
                        in_string = False
                        quote_char = None

                # Handle comments
                if char == "#" and not in_string:
                    # Rest of line is comment, skip it
                    break

                result.append(char)
                i += 1

            # Preserve trailing whitespace context but strip trailing spaces
            result_lines.append("".join(result).rstrip())

        # Join lines, removing empty lines that could break continuation
        final_result = []
        for line in result_lines:
            if line.strip():  # Only keep non-empty lines
                final_result.append(line)

        return "\n".join(final_result)

    def _try_parse_filter_to_sql(self, group_predicate: Callable) -> "Optional[str]":
        """
        Attempt to parse a group filter predicate to SQL with window functions.

        Returns SQL WHERE clause string if successful, None if pattern not supported.

        Supported patterns:
        - g.count() > N, >= N, < N, <= N, == N
        - g.first().col == val, g.last().col == val
        - g.max('col') > val, etc.
        - Combinations: (pred1) & (pred2), (pred1) | (pred2)
        """
        import ast
        import inspect

        try:
            # Get the source code of the lambda
            source = inspect.getsource(group_predicate).strip()
            # Remove 'lambda g: ' prefix
            if "lambda" in source:
                source = source.split(":", 1)[1].strip()

            # Parse to AST
            expr = ast.parse(source, mode="eval").body

            # Recursively convert AST to SQL
            sql = self._ast_to_sql(expr)
            return sql if sql else None
        except Exception as e:
            # If parsing fails, return None to fall back to pandas
            return None

    def _ast_to_sql(self, node):
        """Convert an AST node to SQL WHERE clause."""
        import ast

        if isinstance(node, ast.Compare):
            # Handle comparisons: g.count() > 3
            left_sql = self._ast_to_sql(node.left)
            if not left_sql:
                return None

            # Combine all ops and comparators
            for op, comp in zip(node.ops, node.comparators):
                comp_sql = self._ast_to_sql(comp)
                if not comp_sql:
                    return None

                op_str = self._ast_op_to_sql(op)
                if not op_str:
                    return None

                left_sql = f"{left_sql} {op_str} {comp_sql}"

            return left_sql

        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations: pred1 & pred2, pred1 | pred2
            op_str = " AND " if isinstance(node.op, ast.And) else " OR "
            values = []
            for v in node.values:
                v_sql = self._ast_to_sql(v)
                if not v_sql:
                    return None
                values.append(f"({v_sql})")

            return op_str.join(values)

        elif isinstance(node, ast.Call):
            # Handle function calls: g.count(), g.first().col, g.max('col')
            return self._ast_call_to_sql(node)

        elif isinstance(node, ast.Attribute):
            # Handle attribute access: g.first().col
            return self._ast_attribute_to_sql(node)

        elif isinstance(node, ast.Constant):
            # Handle constants: numbers, strings, True/False, None
            if isinstance(node.value, str):
                return f"'{node.value}'"
            elif isinstance(node.value, bool):
                return "TRUE" if node.value else "FALSE"
            elif node.value is None:
                return "NULL"
            else:
                return str(node.value)

        return None

    def _ast_op_to_sql(self, op):
        """Convert AST operator to SQL operator."""
        import ast

        op_map = {
            ast.Gt: ">",
            ast.GtE: ">=",
            ast.Lt: "<",
            ast.LtE: "<=",
            ast.Eq: "=",
            ast.NotEq: "!=",
        }

        return op_map.get(type(op))

    def _ast_call_to_sql(self, node):
        """Convert AST function call to SQL window function."""
        import ast

        # Check if this is a call on g (the group parameter)
        if not (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "g"
        ):
            return None

        method_name = node.func.attr

        if method_name == "count":
            # g.count() -> COUNT(*) OVER (PARTITION BY __group_id__)
            return "COUNT(*) OVER (PARTITION BY __group_id__)"

        elif method_name in ("first", "last"):
            # These need chaining with attribute access, handle separately
            # Return a marker that will be handled in _ast_attribute_to_sql
            return None

        elif method_name in ("max", "min", "sum", "avg"):
            # g.max('col') -> MAX(col) OVER (PARTITION BY __group_id__)
            if len(node.args) != 1:
                return None

            # Get column name from argument
            col_arg = node.args[0]
            if isinstance(col_arg, ast.Constant):
                col_name = col_arg.value
            else:
                return None

            func_upper = method_name.upper()
            return f"{func_upper}({col_name}) OVER (PARTITION BY __group_id__)"

        return None

    def _ast_attribute_to_sql(self, node):
        """Convert AST attribute access to SQL."""
        import ast

        # Handle g.first().col or g.last().col
        if isinstance(node.value, ast.Call):
            call_node = node.value
            if (
                isinstance(call_node.func, ast.Attribute)
                and isinstance(call_node.func.value, ast.Name)
                and call_node.func.value.id == "g"
            ):
                method_name = call_node.func.attr
                col_name = node.attr

                if method_name == "first":
                    # g.first().col -> FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)
                    return f"FIRST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__)"

                elif method_name == "last":
                    # g.last().col -> LAST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)
                    return f"LAST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"

        return None

    def filter(self, group_predicate: Callable) -> "LTSeq":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last(), g.max('col'), etc.

        Returns:
            An LTSeq with only rows from groups that pass the predicate

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
            >>> grouped.filter(lambda g: g.first().price > 100)
            >>> grouped.filter(lambda g: (g.count() > 2) & (g.first().is_up == True))

        Supported predicates:
        - g.count() > N, >= N, < N, <= N, == N
        - g.first().column op value
        - g.last().column op value
        - g.max('column') op value
        - g.min('column') op value
        - g.sum('column') op value
        - g.avg('column') op value
        - Combinations: (predicate1) & (predicate2), (predicate1) | (predicate2)
        """
        # Try SQL-based approach first
        try:
            where_clause = self._try_parse_filter_to_sql(group_predicate)
            if where_clause:
                return self._filter_via_sql(where_clause)
        except Exception:
            # Fall back to pandas if SQL approach fails
            pass

        # Fall back to pandas-based evaluation
        return self._filter_via_pandas(group_predicate)

    def _filter_via_sql(self, where_clause: str) -> "LTSeq":
        """
        Filter using SQL WHERE clause with window functions.

        This is much faster than pandas iteration when the predicate
        can be expressed as SQL. Uses Rust/DataFusion for all processing.
        """
        # Get flattened data with __group_id__, __group_count__, __rn__ columns from Rust
        flattened = self.flatten()

        # Apply SQL filter using Rust's filter_where
        filtered_inner = flattened._inner.filter_where(where_clause)

        # Create result LTSeq
        result_ltseq = self._ltseq.__class__()
        result_ltseq._inner = filtered_inner

        # Update schema (includes internal columns)
        result_ltseq._schema = flattened._schema.copy()

        # Return a new NestedTable with the same grouping lambda
        # This allows chaining filter().derive() in the same grouping context
        result_nested = NestedTable(result_ltseq, self._grouping_lambda)
        return result_nested

    def _filter_via_pandas(self, group_predicate: Callable) -> "LTSeq":
        """
        Filter using pandas iteration (fallback for complex predicates).

        This is the original pandas-based approach, kept for compatibility
        with predicates that can't be expressed in SQL.
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "filter() requires pandas. Install it with: pip install pandas"
            )

        # Convert to pandas using the original table
        df = self._ltseq.to_pandas()

        # Add __group_id__ column by computing grouping for each row
        group_values = []
        group_id = 0
        prev_group_val = None

        for idx, row in df.iterrows():
            # Call the grouping lambda on this row
            row_proxy = RowProxy(row.to_dict())
            group_val = self._grouping_lambda(row_proxy)

            # If group value changed, increment group_id
            if prev_group_val is None or group_val != prev_group_val:
                group_id += 1
                prev_group_val = group_val

            group_values.append(group_id)

        df["__group_id__"] = group_values
        df["__group_count__"] = df.groupby("__group_id__").transform("size")

        # Group by __group_id__ and evaluate predicate for each group
        group_by_id = df.groupby("__group_id__")
        passing_group_ids = set()

        for group_id, group_df in group_by_id:
            # Create a GroupProxy for this group
            group_proxy = GroupProxy(group_df, self)

            # Evaluate the predicate
            try:
                result = group_predicate(group_proxy)
                if result:
                    passing_group_ids.add(group_id)
            except AttributeError as e:
                # Provide helpful error message with supported methods
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
                    f"Error evaluating filter predicate on group {group_id}: {e}\n{supported_methods}"
                )
            except Exception as e:
                raise ValueError(
                    f"Error evaluating filter predicate on group {group_id}: {e}"
                )

        # Filter the dataframe to keep only passing groups
        filtered_df = df[df["__group_id__"].isin(passing_group_ids)]

        # Store the group assignments for this filtered result
        # Map from row index in original data to group_id
        group_assignments = {}
        for original_idx, (_, row) in enumerate(filtered_df.iterrows()):
            group_assignments[original_idx] = int(row["__group_id__"])

        # Remove internal columns
        result_cols = [c for c in filtered_df.columns if not c.startswith("__")]
        result_df = filtered_df[result_cols]

        # Convert back to LTSeq using _from_rows
        rows = result_df.to_dict("records")
        schema = self._ltseq._schema.copy()

        result_ltseq = self._ltseq.__class__._from_rows(rows, schema)

        # Return a new NestedTable with the same grouping lambda
        # This allows chaining filter().derive() in the same grouping context
        result_nested = NestedTable(result_ltseq, self._grouping_lambda)
        result_nested._group_assignments = group_assignments
        return result_nested

    def _get_unsupported_filter_error(self, source: str) -> str:
        """Generate detailed error message for unsupported filter patterns."""
        return f"""Unsupported filter predicate pattern.

Expression:
  {source.strip()}

Supported patterns:
  - g.count() > N, g.count() >= N, g.count() < N, g.count() <= N, g.count() == N
  - g.first().column == value, g.first().column > value, etc.
  - g.last().column == value, g.last().column < value, etc.
  - g.max('column') >= value, g.min('column') <= value, etc.
  - g.sum('column') > value, g.avg('column') == value
  - Combinations: (g.count() > 2) & (g.first().is_up == True)
  - Multiple combinations: ((g.count() > 2) & (g.first().col == val)) | (g.max('col') > 100)

Supported comparison operators: >, <, >=, <=, ==, !=
Supported boolean operators: & (and), | (or)

Unsupported:
  - Row-level operations (e.g., g.filter(...), g.select(...))
  - Unsupported group functions (e.g., g.median(), g.mode())
  - Complex expressions without explicit comparisons
  - Nested function calls beyond first().col or last().col

Tip: Try breaking complex predicates into simpler patterns or file an issue."""

    def _get_parse_error_message(self, source: str, error: str) -> str:
        """Generate error message for syntax errors in filter predicate."""
        return f"""Failed to parse filter predicate.

Expression:
  {source.strip() if source else "<unavailable>"}

Error: {error}

Make sure your lambda:
  1. Is syntactically valid Python
  2. Takes exactly one parameter (the group)
  3. Returns a boolean expression
  4. Uses supported group operations

Example of valid syntax:
  lambda g: (g.count() > 2) & (g.first().price > 100)"""

    def _extract_filter_condition(self, ast_tree) -> str:
        """
        Extract a WHERE clause from an AST tree of a filter lambda.

        Handles patterns like:
        - lambda g: g.count() > 2
        - lambda g: g.first().price > 100
        - lambda g: (g.count() > 2) & (g.first().is_up == True)

        Returns empty string if pattern not supported, which triggers detailed error.
        """
        import ast

        # Find the main body of the lambda (should be a single expression)
        lambda_body = None
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.Lambda):
                lambda_body = node.body
                break

        if lambda_body is None:
            return ""

        try:
            # Process the body expression (handles BoolOp, Compare, etc.)
            return self._process_ast_expr(lambda_body)
        except Exception as e:
            # Return empty string to trigger error handling in filter()
            return ""

    def _process_ast_expr(self, expr) -> str:
        """
        Recursively process AST expression to build SQL WHERE clause.

        Handles:
        - Compare nodes: g.count() > 2, g.first().col == value, etc.
        - BoolOp nodes: (expr1) & (expr2), (expr1) | (expr2)
        """
        import ast

        if isinstance(expr, ast.Compare):
            return self._process_compare(expr)
        elif isinstance(expr, ast.BoolOp):
            return self._process_boolop(expr)
        else:
            return ""

    def _process_compare(self, compare_node) -> str:
        """Process a Compare node (e.g., g.count() > 2)."""
        import ast

        left = compare_node.left
        op = compare_node.ops[0]
        comparator = compare_node.comparators[0]

        # Get the operator symbol
        op_str = self._get_operator_str(op)
        if not op_str:
            return ""

        # Get the right-hand value
        rhs_value = self._get_literal_value(comparator)
        if rhs_value is None:
            return ""

        # Determine what's on the left side
        if isinstance(left, ast.Call):
            # Could be g.count(), g.first().col, g.last().col, g.max('col'), etc.
            return self._process_call_comparison(left, op_str, rhs_value)
        elif isinstance(left, ast.Attribute):
            # Could be chained: g.first().col or g.last().col
            # The attribute's value should be a Call to first() or last()
            return self._process_chained_attribute_comparison(left, op_str, rhs_value)
        else:
            return ""

    def _process_call_comparison(self, call_node, op_str: str, rhs_value) -> str:
        """Process a function call on left side of comparison (e.g., g.count() > 2)."""
        import ast

        # call_node.func should be an Attribute (the method being called)
        if not isinstance(call_node.func, ast.Attribute):
            return ""

        method_name = call_node.func.attr

        # Handle g.count()
        if method_name == "count" and len(call_node.args) == 0:
            return f"__group_count__ {op_str} {rhs_value}"

        # Handle g.first().col, g.last().col, g.max('col'), etc.
        if method_name in ("first", "last", "max", "min", "sum", "avg"):
            return self._process_group_function(
                method_name, call_node, op_str, rhs_value
            )

        return ""

    def _process_chained_attribute_comparison(
        self, attr_node, op_str: str, rhs_value
    ) -> str:
        """
        Process a chained attribute comparison like g.first().is_up == True.

        The attr_node should have:
        - attr: the column name (e.g., 'is_up')
        - value: a Call node to first() or last()
        """
        import ast

        column_name = attr_node.attr
        value_node = attr_node.value

        # Check if value_node is a Call to first() or last()
        if not isinstance(value_node, ast.Call):
            return ""

        if not isinstance(value_node.func, ast.Attribute):
            return ""

        method_name = value_node.func.attr
        if method_name not in ("first", "last"):
            return ""

        # Get the object that first/last is called on (should be 'g')
        if not isinstance(value_node.func.value, ast.Name):
            return ""

        # Now generate the SQL for first(col) or last(col)
        if method_name == "first":
            return f"(CASE WHEN ROW_NUMBER() OVER (PARTITION BY __group_id__) = 1 THEN {column_name} END) {op_str} {rhs_value}"
        else:  # last
            return f"(CASE WHEN ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY rowid DESC) = 1 THEN {column_name} END) {op_str} {rhs_value}"

    def _process_group_function(
        self, method_name: str, call_node, op_str: str, rhs_value
    ) -> str:
        """Process group functions like first().col or max('col')."""
        import ast

        # For first().col or last().col pattern
        if method_name in ("first", "last"):
            # call_node.func.value should be 'g' (the object the method is called on)
            # But we're looking at a chained call: g.first().col
            # We need to look at the parent Compare node's left side more carefully

            # This is actually a chained attribute access
            # We need to walk the attribute chain
            return ""

        # For max('col'), min('col'), sum('col'), avg('col')
        if method_name in ("max", "min", "sum", "avg"):
            # Should have one string argument
            if len(call_node.args) == 1 and isinstance(call_node.args[0], ast.Constant):
                col_name = call_node.args[0].value
                if isinstance(col_name, str):
                    agg_func_upper = method_name.upper()
                    return f"{agg_func_upper}({col_name}) OVER (PARTITION BY __group_id__) {op_str} {rhs_value}"

        return ""

    def _process_boolop(self, boolop_node) -> str:
        """Process Boolean operations (& or |)."""
        import ast

        # Determine operator
        if isinstance(boolop_node.op, ast.And):
            op_str = "AND"
        elif isinstance(boolop_node.op, ast.Or):
            op_str = "OR"
        else:
            return ""

        # Process all values in the BoolOp
        parts = []
        for value in boolop_node.values:
            part = self._process_ast_expr(value)
            if part:
                parts.append(f"({part})")
            else:
                # If any part fails, the whole expression fails
                return ""

        if parts:
            return f" {op_str} ".join(parts)
        return ""

    def _get_operator_str(self, op) -> str:
        """Convert AST operator to SQL operator string."""
        import ast

        if isinstance(op, ast.Gt):
            return ">"
        elif isinstance(op, ast.GtE):
            return ">="
        elif isinstance(op, ast.Lt):
            return "<"
        elif isinstance(op, ast.LtE):
            return "<="
        elif isinstance(op, ast.Eq):
            return "="
        elif isinstance(op, ast.NotEq):
            return "!="
        else:
            return ""

    def _get_literal_value(self, node):
        """Extract a literal value from an AST node (number or string)."""
        import ast

        if isinstance(node, ast.Constant):
            value = node.value
            # Return the value as a SQL-safe string
            if isinstance(value, str):
                # Escape single quotes
                escaped = value.replace("'", "''")
                return f"'{escaped}'"
            elif isinstance(value, bool):
                # SQL boolean
                return "true" if value else "false"
            elif isinstance(value, (int, float)):
                return str(value)
            elif value is None:
                return "NULL"

        return None

    def _filter_by_sql(self, flattened, where_clause: str) -> "LTSeq":
        """Apply a SQL WHERE clause to filter the flattened table."""
        # Create a new LTSeq with the filtered result
        result = flattened.__class__()
        result._schema = flattened._schema.copy()

        # Use the Rust filter_where method to execute the SQL WHERE clause
        result._inner = flattened._inner.filter_where(where_clause)

        return result

    def derive(self, group_mapper: Callable) -> "LTSeq":
        """
        Derive new columns based on group properties.

        Each group property (first row, last row, count, etc.) is broadcasted
        to all rows in that group.

        Args:
            group_mapper: Lambda taking a group proxy (g) and returning a dict
                         of new columns. Each column value will be broadcasted
                         to all rows in the group.

        Returns:
            LTSeq with original rows plus new derived columns

        Example:
            >>> grouped.derive(lambda g: {"span": g.count()})
            >>> grouped.derive(lambda g: {
            ...     "start": g.first().date,
            ...     "end": g.last().date,
            ...     "gain": (g.last().price - g.first().price) / g.first().price
            ... })

        Supported expressions:
        - g.count() - row count per group
        - g.first().column - first row value for column
        - g.last().column - last row value for column
        - g.max('column') - maximum value in column per group
        - g.min('column') - minimum value in column per group
        - g.sum('column') - sum of values in column per group
        - g.avg('column') - average value in column per group
        - Arithmetic: (expr1) - (expr2), (expr1) / (expr2), (expr1) + (expr2), etc.
        """
        # Check if we have stored group assignments from a filter operation
        # In this case, we must use pandas to preserve the original group boundaries
        if self._group_assignments is not None:
            return self._derive_via_pandas_with_stored_groups(group_mapper)

        # Materialize the grouping to get __group_id__
        flattened = self.flatten()

        import inspect
        import ast

        source = None
        try:
            # Get the source code of the derive lambda
            try:
                source = inspect.getsource(group_mapper)
            except (OSError, TypeError):
                source = None

            if source:
                # Parse the lambda
                # Need to dedent source since it may be indented (from test methods, etc.)
                import textwrap

                source_dedented = textwrap.dedent(source)

                # If source includes a method chain like .filter(...).derive(lambda...),
                # we need to extract just the lambda part
                source_to_parse = self._extract_lambda_from_chain(source_dedented)

                try:
                    tree = ast.parse(source_to_parse)
                except SyntaxError:
                    # If we can't parse it as is, try wrapping in parentheses for expression parsing
                    try:
                        tree = ast.parse(f"({source_to_parse})", mode="eval")
                    except SyntaxError:
                        raise ValueError(
                            f"Cannot parse derive lambda expression. "
                            f"Source: {source_to_parse}"
                        )

                # Extract derive expressions from the dict literal
                derive_exprs = self._extract_derive_expressions(tree, flattened._schema)

                if derive_exprs:
                    # Use SQL window functions to derive columns
                    return self._derive_via_sql(flattened, derive_exprs)
                else:
                    # Failed to parse or unsupported pattern
                    raise ValueError(self._get_unsupported_derive_error(source))
            else:
                raise ValueError(
                    "Cannot parse derive expression (source not available). "
                    "This can happen with lambdas defined in REPL. "
                    "Please define the lambda in a file for now."
                )
        except ValueError:
            # Re-raise our custom errors
            raise
        except Exception as e:
            # Parse or other error
            source_str = source if source else "<unavailable>"
            raise ValueError(self._get_derive_parse_error_message(source_str, str(e)))

    def _extract_derive_expressions(self, ast_tree, schema) -> Dict[str, str]:
        """
        Extract derive expressions from lambda g: {dict} AST.

        Returns dict mapping column_name -> SQL expression for window functions.
        Returns empty dict if pattern not supported.
        """
        import ast

        # Find the dict literal in the lambda body
        dict_node = None
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.Lambda):
                if isinstance(node.body, ast.Dict):
                    dict_node = node.body
                    break

        if not dict_node:
            return {}

        result = {}

        # Process each key-value pair in the dict
        for key_node, value_node in zip(dict_node.keys, dict_node.values):
            # Key must be a string constant
            if not isinstance(key_node, ast.Constant) or not isinstance(
                key_node.value, str
            ):
                return {}  # Invalid key

            col_name = key_node.value

            # Value should be an expression (Call, BinOp, Attribute, etc.)
            sql_expr = self._process_derive_expr(value_node, schema)
            if not sql_expr:
                return {}  # Unsupported expression

            result[col_name] = sql_expr

        return result

    def _process_derive_expr(self, expr, schema) -> str:
        """
        Process a derive expression and return SQL window function.

        Handles:
        - g.count()
        - g.first().col
        - g.last().col
        - g.max('col'), g.min('col'), g.sum('col'), g.avg('col')
        - BinOp: arithmetic combinations
        """
        import ast

        if isinstance(expr, ast.Call):
            return self._process_derive_call(expr, schema)
        elif isinstance(expr, ast.BinOp):
            return self._process_derive_binop(expr, schema)
        elif isinstance(expr, ast.Attribute):
            # Could be g.first().col chained access
            return self._process_derive_attribute(expr, schema)
        else:
            return ""

    def _process_derive_call(self, call_node, schema) -> str:
        """Process function calls like g.count(), g.max('col'), etc."""
        import ast

        if not isinstance(call_node.func, ast.Attribute):
            return ""

        method_name = call_node.func.attr

        # Handle g.count()
        if method_name == "count" and len(call_node.args) == 0:
            return "COUNT(*) OVER (PARTITION BY __group_id__)"

        # Handle g.max('col'), g.min('col'), g.sum('col'), g.avg('col')
        if method_name in ("max", "min", "sum", "avg"):
            if len(call_node.args) == 1 and isinstance(call_node.args[0], ast.Constant):
                col_name = call_node.args[0].value
                if isinstance(col_name, str):
                    agg_func = method_name.upper()
                    return f"{agg_func}({col_name}) OVER (PARTITION BY __group_id__)"

        # Handle g.first() and g.last() (without chaining)
        if method_name in ("first", "last") and len(call_node.args) == 0:
            # These need to be chained with attribute access (handled in _process_derive_attribute)
            return ""

        return ""

    def _process_derive_attribute(self, attr_node, schema) -> str:
        """
        Process attribute access chains like g.first().col or g.last().col.

        AST structure: Attribute(value=Call(...), attr='col')
        """
        import ast

        # attr_node.value should be a Call (g.first() or g.last())
        if not isinstance(attr_node.value, ast.Call):
            return ""

        inner_call = attr_node.value
        col_name = attr_node.attr

        # Check if inner_call is g.first() or g.last()
        if not isinstance(inner_call.func, ast.Attribute):
            return ""

        method_name = inner_call.func.attr

        if method_name == "first" and len(inner_call.args) == 0:
            return f"FIRST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__)"
        elif method_name == "last" and len(inner_call.args) == 0:
            return f"LAST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"

        return ""

    def _process_derive_binop(self, binop_node, schema) -> str:
        """
        Process binary operations like (expr1) - (expr2), (expr1) / (expr2).

        Recursively processes left and right operands.
        """
        import ast

        # Get operator
        op_str = self._get_binop_str(binop_node.op)
        if not op_str:
            return ""

        # Process operands
        left_sql = self._process_derive_expr(binop_node.left, schema)
        right_sql = self._process_derive_expr(binop_node.right, schema)

        if not left_sql or not right_sql:
            return ""

        # Return combined expression with proper parentheses
        return f"({left_sql} {op_str} {right_sql})"

    def _get_binop_str(self, op) -> str:
        """Convert AST BinOp to SQL operator."""
        import ast

        if isinstance(op, ast.Add):
            return "+"
        elif isinstance(op, ast.Sub):
            return "-"
        elif isinstance(op, ast.Mult):
            return "*"
        elif isinstance(op, ast.Div):
            return "/"
        elif isinstance(op, ast.FloorDiv):
            return "/"  # SQL floor division
        elif isinstance(op, ast.Mod):
            return "%"
        else:
            return ""

    def _derive_via_sql(
        self, flattened: "LTSeq", derive_exprs: Dict[str, str]
    ) -> "LTSeq":
        """
        Apply derived column expressions via SQL SELECT with window functions.

        This method uses Rust/DataFusion to execute the SQL window functions,
        providing much better performance than the pandas-based approach.

        The flattened LTSeq already has __group_id__ and __rn__ columns from
        the flatten() call in derive().

        Args:
            flattened: LTSeq with __group_id__ and __rn__ columns
            derive_exprs: Dict mapping column name -> SQL expression
                         e.g., {"span": "COUNT(*) OVER (PARTITION BY __group_id__)"}

        Returns:
            New LTSeq with derived columns (and __group_id__, __rn__ removed)
        """
        # Call Rust's derive_window_sql() with the SQL expressions
        # This will:
        # 1. Execute the window functions via DataFusion SQL
        # 2. Add the derived columns
        # 3. Remove __group_id__ and __rn__ columns
        result = flattened.__class__()

        # Build schema: original columns (without internal) + derived columns
        result._schema = {}
        for col_name, col_type in self._ltseq._schema.items():
            if col_name not in ("__group_id__", "__rn__", "__group_count__"):
                result._schema[col_name] = col_type
        for col_name in derive_exprs.keys():
            result._schema[col_name] = "Unknown"

        # Call Rust to execute the window functions
        result._inner = flattened._inner.derive_window_sql(derive_exprs)

        return result

    def _derive_via_pandas_with_stored_groups(self, group_mapper: Callable) -> "LTSeq":
        """
        Derive columns using pandas when we have stored group assignments from filter().

        This is used when derive() is called on a NestedTable that came from filter().
        The stored _group_assignments preserve the original group boundaries that would
        be lost if we recomputed groups from the grouping lambda on the filtered data.

        Args:
            group_mapper: Lambda taking a group proxy (g) and returning a dict
                         of new columns.

        Returns:
            LTSeq with derived columns added
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "derive() with stored group assignments requires pandas. "
                "Install it with: pip install pandas"
            )

        import inspect
        import ast
        import textwrap

        # Get the source code of the derive lambda
        try:
            source = inspect.getsource(group_mapper)
        except (OSError, TypeError):
            raise ValueError(
                "Cannot parse derive expression (source not available). "
                "This can happen with lambdas defined in REPL. "
                "Please define the lambda in a file for now."
            )

        # Parse the lambda to extract SQL expressions
        source_dedented = textwrap.dedent(source)
        source_to_parse = self._extract_lambda_from_chain(source_dedented)

        try:
            tree = ast.parse(source_to_parse)
        except SyntaxError:
            try:
                tree = ast.parse(f"({source_to_parse})", mode="eval")
            except SyntaxError:
                raise ValueError(
                    f"Cannot parse derive lambda expression. Source: {source_to_parse}"
                )

        # Extract derive expressions
        schema = self._ltseq._schema.copy()
        derive_exprs = self._extract_derive_expressions(tree, schema)

        if not derive_exprs:
            raise ValueError(self._get_unsupported_derive_error(source))

        # Convert to pandas and add group columns
        df = self._ltseq.to_pandas()

        # Add __rn__ (row number) column for window function ordering
        df["__rn__"] = range(len(df))

        # Add __group_id__ column using stored assignments
        group_values = [self._group_assignments[i] for i in range(len(df))]
        df["__group_id__"] = group_values

        # Evaluate each derive expression using pandas
        for col_name, sql_expr in derive_exprs.items():
            try:
                df[col_name] = self._evaluate_sql_expr_in_pandas(df, sql_expr)
            except Exception as e:
                raise ValueError(f"Error evaluating derived column '{col_name}': {e}")

        # Remove the internal columns before returning
        df = df.drop(columns=["__group_id__", "__rn__"])

        # Convert back to LTSeq
        result_schema = self._ltseq._schema.copy()
        for col_name in derive_exprs.keys():
            result_schema[col_name] = "Unknown"

        rows = df.to_dict("records")
        result = self._ltseq.__class__._from_rows(rows, result_schema)

        return result

    def _evaluate_sql_expr_in_pandas(self, df, sql_expr: str):
        """
        Evaluate SQL-like window function expression in pandas.

        Simple implementation for common patterns.
        Examples:
        - COUNT(*) OVER (PARTITION BY __group_id__)
        - FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)
        - (expr1) - (expr2) where expr1 and expr2 are window functions
        """
        import re

        # Handle arithmetic expressions recursively
        # First extract the inner expression without outer parens
        if sql_expr.startswith("(") and sql_expr.endswith(")"):
            inner = sql_expr[1:-1]

            # Try to find a top-level operator not inside window functions
            # Count parens to find where operators are at top level
            paren_depth = 0
            for i, char in enumerate(inner):
                if char == "(":
                    paren_depth += 1
                elif char == ")":
                    paren_depth -= 1
                elif paren_depth == 0 and char in ["+", "-", "*", "/"]:
                    # Found top-level operator
                    left_expr = inner[:i].strip()
                    op = char
                    right_expr = inner[i + 1 :].strip()

                    # Recursively evaluate sub-expressions
                    left_result = self._evaluate_sql_expr_in_pandas(df, left_expr)
                    right_result = self._evaluate_sql_expr_in_pandas(df, right_expr)

                    # Apply operation
                    if op == "+":
                        return left_result + right_result
                    elif op == "-":
                        return left_result - right_result
                    elif op == "*":
                        return left_result * right_result
                    elif op == "/":
                        return left_result / right_result

        # Handle COUNT(*) OVER (PARTITION BY __group_id__)
        if "COUNT(*)" in sql_expr and "PARTITION BY __group_id__" in sql_expr:
            return df.groupby("__group_id__").transform("size")

        # Handle FIRST_VALUE(col) OVER (...)
        if "FIRST_VALUE(" in sql_expr:
            match = re.search(r"FIRST_VALUE\((\w+)\)", sql_expr)
            if match:
                col_name = match.group(1)
                return df.groupby("__group_id__")[col_name].transform("first")

        # Handle LAST_VALUE(col) OVER (...)
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

        # Default: return the expr as-is (will likely fail)
        return df.eval(sql_expr)

    def _get_unsupported_derive_error(self, source: str) -> str:
        """Generate error for unsupported derive patterns."""
        return f"""Unsupported derive expression pattern.

Expression:
  {source.strip()}

Supported expressions:
  - g.count() → COUNT(*) OVER (PARTITION BY __group_id__)
  - g.first().column → FIRST_VALUE(column) OVER (...)
  - g.last().column → LAST_VALUE(column) OVER (...)
  - g.max('column') → MAX(column) OVER (PARTITION BY __group_id__)
  - g.min('column') → MIN(column) OVER (...)
  - g.sum('column') → SUM(column) OVER (...)
  - g.avg('column') → AVG(column) OVER (...)
  - Arithmetic: (expr1) - (expr2), (expr1) / (expr2), (expr1) + (expr2), (expr1) * (expr2)

Unsupported:
  - Row-level operations (e.g., g.filter(...), g.select(...))
  - Aggregate functions beyond max/min/sum/avg (e.g., g.median())
  - Complex nested expressions
  - Non-arithmetic operations between group properties

Example of valid syntax:
  lambda g: {{
      "span": g.count(),
      "start": g.first().date,
      "end": g.last().date,
      "gain": (g.last().price - g.first().price) / g.first().price
  }}"""

    def _get_derive_parse_error_message(self, source: str, error: str) -> str:
        """Generate error for syntax/parse errors in derive lambda."""
        return f"""Failed to parse derive expression.

Expression:
  {source.strip() if source else "<unavailable>"}

Error: {error}

Make sure your lambda:
  1. Is syntactically valid Python
  2. Takes exactly one parameter (the group)
  3. Returns a dict with string keys and value expressions
  4. Values are supported group operations or arithmetic combinations

Example:
  lambda g: {{"span": g.count(), "gain": (g.last().price - g.first().price) / g.first().price}}"""
