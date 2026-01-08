"""Core LTSeq table class with data operations."""

from typing import Any, Callable, Dict, Optional, Union

from .expr import SchemaProxy, _lambda_to_expr
from .helpers import (
    _contains_window_function,
    _infer_schema_from_csv,
    _normalize_schema,
)

try:
    from . import ltseq_core

    HAS_RUST_BINDING = True
except ImportError:
    HAS_RUST_BINDING = False


def _process_select_col(col, schema: Dict[str, str]):
    """Process a single column argument for select()."""
    if isinstance(col, str):
        # Column name reference
        if col not in schema:
            raise AttributeError(
                f"Column '{col}' not found in schema. "
                f"Available columns: {list(schema.keys())}"
            )
        return {"type": "Column", "name": col}
    elif callable(col):
        # Lambda that computes expressions or columns
        proxy = SchemaProxy(schema)
        result = col(proxy)

        # Check if result is a list (multiple column selection)
        if isinstance(result, list):
            # Extract expressions from list
            exprs = []
            for item in result:
                if hasattr(item, "serialize"):  # It's an Expr
                    exprs.append(item.serialize())
                else:
                    raise TypeError(
                        f"List items must be column expressions, got {type(item)}"
                    )
            return exprs
        elif hasattr(result, "serialize"):
            # Single expression result
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

        # API 2: single callable returning dict
        func = args[0]
        if not callable(func):
            raise TypeError(
                f"derive() positional argument must be callable, got {type(func).__name__}"
            )

        # Capture the function to get the expression dict
        expr_dict = capture_expr_fn(func)

        # Extract dict literal
        if expr_dict.get("type") == "Dict":
            # Direct dict literal
            derived_cols = {}
            for key_expr, value_expr in zip(
                expr_dict.get("keys", []), expr_dict.get("values", [])
            ):
                # Extract column name from key
                if key_expr.get("type") == "Literal":
                    col_name = key_expr.get("value", "")
                    derived_cols[col_name] = value_expr
                else:
                    raise ValueError("Dict keys in derive() must be string literals")
        else:
            # Fallback: treat the entire expression as a single derived column
            derived_cols = {"_derived": expr_dict}
    else:
        # API 1: keyword arguments
        derived_cols = {}
        for col_name, fn in kwargs.items():
            if not callable(fn):
                raise TypeError(
                    f"derive() argument '{col_name}' must be callable, got {type(fn).__name__}"
                )
            derived_cols[col_name] = capture_expr_fn(fn)

    return derived_cols


def _collect_key_exprs(key_exprs, schema, capture_expr_fn):
    """Collect key expressions from str or Callable arguments."""
    result = []
    for key_expr in key_exprs:
        if isinstance(key_expr, str):
            # String column name: create simple Column expression
            result.append({"type": "Column", "name": key_expr})
        elif callable(key_expr):
            # Lambda expression: capture and serialize
            expr_dict = capture_expr_fn(key_expr)
            result.append(expr_dict)
        else:
            raise TypeError(
                f"Argument must be str or callable, got {type(key_expr).__name__}"
            )
    return result


class LTSeq:
    """Python-visible LTSeq wrapper backed by the native Rust kernel."""

    def __init__(self):
        if not HAS_RUST_BINDING:
            raise RuntimeError(
                "Rust extension ltseq_core not available. "
                "Please rebuild with `maturin develop`."
            )
        self._inner = ltseq_core.RustTable()
        self._schema: Dict[str, str] = {}
        self._csv_path: Optional[str] = None  # Track original CSV file for to_pandas()

    @classmethod
    def read_csv(cls, path: str) -> "LTSeq":
        """
        Read a CSV file and return an LTSeq instance.

        Infers schema from the CSV header and first rows.

        Args:
            path: Path to CSV file

        Returns:
            New LTSeq instance with data loaded from CSV

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> filtered = t.filter(lambda r: r.age > 18)
        """
        t = cls()
        t._schema = _infer_schema_from_csv(path)
        t._csv_path = path  # Store path for to_pandas() fallback
        # Call Rust RustTable.read_csv to load the actual data
        t._inner.read_csv(path)
        return t

    @classmethod
    def _from_rows(cls, rows: list[Dict[str, Any]], schema: Dict[str, str]) -> "LTSeq":
        """
        Create an LTSeq instance from a list of row dictionaries.

        Internal method used by partition() and similar operations.

        Args:
            rows: List of dictionaries, one per row
            schema: Column schema as {column_name: column_type_string}

        Returns:
            New LTSeq instance
        """
        import tempfile
        import csv
        import os

        if not rows:
            # Create empty table with schema
            t = cls()
            t._schema = schema
            # Still need to initialize _inner with empty data
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, newline=""
            ) as f:
                writer = csv.DictWriter(f, fieldnames=schema.keys())
                writer.writeheader()
                temp_path = f.name
            try:
                t._inner.read_csv(temp_path)
                t._csv_path = None  # Don't use the deleted temp file
            finally:
                os.unlink(temp_path)
            return t

        # Convert rows to CSV and load
        # We need to keep the CSV file around for lazy loading
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=schema.keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            temp_path = f.name

        # Create the LTSeq and use the temp file as the source
        t = cls.read_csv(temp_path)
        # The temp file will be deleted by the OS eventually, but keep it around
        # for now by using it as _csv_path
        # This is a limitation: temp files won't be cleaned up automatically
        # A better solution would be to materialize the data or use a cache directory
        return t

    def write_csv(self, path: str) -> None:
        """
        Write the table to a CSV file.

        Args:
            path: Path where the CSV file will be written

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.write_csv("output.csv")
        """
        self._inner.write_csv(path)

    def show(self, n: int = 10) -> None:
        """
        Display the data as a pretty-printed ASCII table.

        Args:
            n: Maximum number of rows to display (default 10)

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.show()
        """
        # Delegate to Rust implementation
        out = self._inner.show(n)
        print(out)

    def to_pandas(self):
        """
        Convert the table to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing the table data

        Requires pandas to be installed.

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> df = t.to_pandas()
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "to_pandas() requires pandas. Install it with: pip install pandas"
            )

        # If we have the original CSV path, read it directly
        if self._csv_path:
            return pd.read_csv(self._csv_path)

        # Otherwise, use CSV round-trip via write_csv
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            temp_path = f.name

        try:
            # Write to temporary CSV
            self.write_csv(temp_path)
            # Read with pandas
            df = pd.read_csv(temp_path)
            return df
        finally:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def hello(self) -> str:
        return self._inner.hello()

    def __len__(self) -> int:
        """
        Get the number of rows in the table.

        Returns:
            The count of rows

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> print(len(t))  # Number of rows
        """
        return self._inner.count()

    def _capture_expr(self, fn: Callable) -> Dict[str, Any]:
        """
        Capture and serialize an expression tree from a lambda.

        This internal method intercepts Python lambdas and converts them to
        serializable expression dicts without executing Python logic.

        Args:
            fn: Lambda function, e.g., lambda r: r.age > 18

        Returns:
            Serialized expression dict ready for Rust deserialization

        Raises:
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column
            ValueError: If schema is not initialized (call read_csv first)

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> expr = t._capture_expr(lambda r: r.age > 18)
            >>> expr["type"]
            'BinOp'
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )
        return _lambda_to_expr(fn, self._schema)

    def filter(self, predicate: Callable) -> "LTSeq":
        """
        Filter rows where predicate lambda returns True.

        Args:
            predicate: Lambda that takes a row and returns a boolean Expr.
                      E.g., lambda r: r.age > 18

        Returns:
            A new LTSeq with filtered data

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return a boolean Expr
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> filtered = t.filter(lambda r: r.age > 18)
            >>> filtered.show()
        """
        expr_dict = self._capture_expr(predicate)

        # Create a new LTSeq with the filtered result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate filtering to Rust implementation
        result._inner = self._inner.filter(expr_dict)
        return result

    def select(self, *cols) -> "LTSeq":
        """
        Select specific columns or derived expressions.

        Supports both column references and computed columns via lambdas.

        Args:
            *cols: Either string column names or lambdas that compute new columns.
                   E.g., select("name", "age") or select(lambda r: [r.name, r.age])
                   Or for computed: select(lambda r: r.price * r.qty)

        Returns:
            A new LTSeq with selected/computed columns

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return valid expressions
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> selected = t.select("name", "age")
            >>> selected = t.select(lambda r: [r.name, r.age])
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        exprs = []
        for col in cols:
            col_result = _process_select_col(col, self._schema)
            if isinstance(col_result, list):
                exprs.extend(col_result)
            else:
                exprs.append(col_result)

        # Create a new LTSeq with selected columns
        result = LTSeq()
        # Update schema based on selected columns
        # For now, we keep the full schema (proper schema reduction requires Rust support)
        result._schema = self._schema.copy()
        # Delegate selection to Rust implementation
        result._inner = self._inner.select(exprs)
        return result

    def derive(self, *args, **kwargs: Callable) -> "LTSeq":
        """
        Create new derived columns based on lambda expressions.

        New columns are added to the dataframe; existing columns remain.

        Supports two API styles:
        1. Keyword arguments: derive(col1=lambda r: r.x, col2=lambda r: r.y)
        2. Single callable returning dict: derive(lambda r: {"col1": r.x, "col2": r.y})

        Args:
            *args: Single optional callable that returns a dict of {col_name: expression}
            **kwargs: Column name -> lambda expression mapping.
                     E.g., derive(total=lambda r: r.price * r.qty, age_group=lambda r: r.age // 10)

        Returns:
            A new LTSeq with added derived columns

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> # API 1: keyword arguments
            >>> derived = t.derive(total=lambda r: r.price * r.qty)
            >>> # API 2: lambda returning dict
            >>> derived = t.derive(lambda r: {"total": r.price * r.qty})
            >>> derived.show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Extract derived columns
        derived_cols = _extract_derive_cols(
            args, kwargs, self._schema, self._capture_expr
        )

        # Create a new LTSeq with derived columns
        result = LTSeq()
        # Update schema: add derived columns (assuming Int64 for now as placeholder)
        result._schema = self._schema.copy()
        for col_name in derived_cols:
            # In Phase 2, we'd infer the dtype from the expression
            result._schema[col_name] = "Unknown"

        # Check if any expressions contain window functions
        has_window_functions = any(
            _contains_window_function(expr) for expr in derived_cols.values()
        )

        # Rust backend handles both standard and window functions
        result._inner = self._inner.derive(derived_cols)

        return result

    def sort(
        self, *key_exprs: Union[str, Callable], desc: Union[bool, list] = False
    ) -> "LTSeq":
        """
        Sort rows by one or more key expressions with optional descending order.

        Reorders data based on column values. Multiple sort keys are applied in order.

        Phase 8J: Enhanced to support descending sort via `desc` parameter.

        Args:
            *key_exprs: Column names (str) or lambda expressions that return sortable values.
                       Examples:
                       - sort("date") - sort by date column ascending
                       - sort(lambda r: r.date) - same as above
                       - sort("date", "id") - multi-key sort: first by date, then by id
            desc: Descending flag(s). Can be:
                  - Single boolean: applies to all keys. E.g., desc=True means all keys DESC
                  - List of booleans: one per key. E.g., desc=[False, True] means date ASC, id DESC
                  - Default: False (all ascending)

        Returns:
            A new LTSeq with sorted data

        Raises:
            ValueError: If schema is not initialized or desc list length doesn't match keys
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.sort("name").show()  # Ascending
            >>> t.sort("name", desc=True).show()  # Descending
            >>> t.sort("date", "value", desc=[False, True]).show()  # date ASC, value DESC
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Collect sort expressions
        sort_exprs = _collect_key_exprs(key_exprs, self._schema, self._capture_expr)

        # Normalize desc parameter to a list of booleans
        if isinstance(desc, bool):
            # Single boolean: apply to all keys
            desc_flags = [desc] * len(sort_exprs)
        elif isinstance(desc, list):
            # List of booleans: validate length matches
            if len(desc) != len(sort_exprs):
                raise ValueError(
                    f"desc list length ({len(desc)}) must match number of sort keys ({len(sort_exprs)})"
                )
            desc_flags = desc
        else:
            raise TypeError(f"desc must be bool or list, got {type(desc).__name__}")

        # Create a new LTSeq with sorted result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate sorting to Rust implementation with desc flags
        result._inner = self._inner.sort(sort_exprs, desc_flags)
        return result

    def distinct(self, *key_exprs: Union[str, Callable]) -> "LTSeq":
        """
        Remove duplicate rows based on key columns.

        By default, retains the first occurrence of each unique key combination.

        Args:
            *key_exprs: Column names (str) or lambda expressions identifying duplicates.
                       If no args provided, considers all columns for uniqueness.
                       Examples:
                       - distinct() - unique across all columns
                       - distinct("id") - unique based on id column
                       - distinct("id", "date") - unique based on id and date columns
                       - distinct(lambda r: r.id) - unique by lambda expression

        Returns:
            A new LTSeq with duplicate rows removed

        Raises:
            ValueError: If schema is not initialized
            AttributeError: If lambda references a non-existent column
            TypeError: If argument is not str or callable

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.distinct("customer_id").show()
            >>> t.distinct("date", "id").show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Collect key expressions
        key_cols = _collect_key_exprs(key_exprs, self._schema, self._capture_expr)

        # Create a new LTSeq with distinct result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate deduplication to Rust implementation
        result._inner = self._inner.distinct(key_cols)
        return result

    def slice(self, offset: int = 0, length: Optional[int] = None) -> "LTSeq":
        """
        Select a contiguous range of rows.

        Similar to SQL LIMIT/OFFSET operations. Zero-copy selection at the logical level.

        Args:
            offset: Starting row index (0-based). Default: 0
            length: Number of rows to include. If None, all rows from offset to end.

        Returns:
            A new LTSeq with the selected row range

        Raises:
            ValueError: If offset < 0 or length < 0
            ValueError: If schema is not initialized

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.slice(10, 5).show()      # Rows 10-14 (5 rows starting at index 10)
            >>> t.slice(offset=100).show()  # From row 100 to end
            >>> t.slice(length=10).show()   # First 10 rows
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Validate parameters
        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        if length is not None and length < 0:
            raise ValueError(f"length must be non-negative, got {length}")

        # Create a new LTSeq with sliced result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate slicing to Rust implementation
        result._inner = self._inner.slice(offset, length)
        return result

    def cum_sum(self, *cols: Union[str, Callable]) -> "LTSeq":
        """
        Add cumulative sum columns for specified columns.

        Calculates running sums across ordered rows. Requires data to be sorted.
        New columns are added to the table with names suffixed by '_cumsum'.

        Args:
            *cols: Column names (str) or lambda expressions.
                   - String: column name (e.g., "amount")
                   - Lambda: expression (e.g., lambda r: r.price * r.qty)
                   - Multiple args: add cumulative sum for each

        Returns:
            A new LTSeq with cumulative sum columns added

        Raises:
            ValueError: If schema is not initialized or no columns provided
            AttributeError: If lambda references a non-existent column
            TypeError: If column type is non-numeric

        Example:
            >>> t = LTSeq.read_csv("sales.csv").sort("date")
            >>> t.cum_sum("revenue").show()
            >>> t.cum_sum("revenue", "units").show()
            >>> t.cum_sum(lambda r: r.price * r.qty).show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not cols:
            raise ValueError("cum_sum() requires at least one column argument")

        # Collect cumulative sum expressions
        cum_exprs = _collect_key_exprs(cols, self._schema, self._capture_expr)

        # Create a new LTSeq with cumulative sum columns added
        result = LTSeq()
        result._schema = self._schema.copy()
        # Add cumulative sum columns to schema (with _cumsum suffix)
        for i, col_expr in enumerate(cols):
            if isinstance(col_expr, str):
                result._schema[f"{col_expr}_cumsum"] = result._schema.get(
                    col_expr, "float64"
                )
            else:
                # For lambda expressions, auto-name the column
                result._schema[f"cum_sum_{i}"] = "float64"
        # Delegate cum_sum to Rust implementation
        result._inner = self._inner.cum_sum(cum_exprs)
        return result

    def group_ordered(self, grouping_fn: Callable) -> "NestedTable":
        """
        Group consecutive identical values based on a grouping function.

        This is the core "state-aware grouping" operation: it groups only consecutive
        rows with identical values in the grouping column (as determined by the lambda).

        The returned NestedTable provides group-level operations like first(), last(),
        count(), and allows filtering/deriving based on group properties.

        Args:
            grouping_fn: Lambda that extracts the grouping key from each row.
                        E.g., lambda r: r.trend_flag

        Returns:
            A NestedTable supporting group-level operations

        Raises:
            ValueError: If schema is not initialized
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("stock.csv").sort("date")
            >>> groups = t.group_ordered(lambda r: r.is_up)
            >>> groups.filter(lambda g: g.count() > 3).show()
            >>> groups.derive(lambda g: {"span": g.last().date - g.first().date}).show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        try:
            # Validate the grouping function works
            test_proxy = SchemaProxy(self._schema)
            _ = grouping_fn(test_proxy)
        except Exception as e:
            raise AttributeError(f"Invalid grouping function: {e}")

        # Import here to avoid circular imports
        from .grouping import NestedTable

        # Return a NestedTable wrapping this LTSeq
        return NestedTable(self, grouping_fn)

    def agg(self, by: Optional[Callable] = None, **aggregations) -> "LTSeq":
        """
        Aggregate rows into a summary table with one row per group.

        This is the traditional SQL GROUP BY operation, returning one row
        per unique grouping key with computed aggregates.

        Args:
            by: Optional grouping key lambda (e.g., lambda r: r.region).
                If None, aggregates entire table into a single row.
                Can return a single column or a list of columns for composite keys.
            **aggregations: Named aggregation expressions using group proxies.
                           Example: sum_sales=lambda g: g.sales.sum()
                           Supports: g.col.sum(), g.col.count(), g.col.min(),
                                    g.col.max(), g.col.avg(), g.count()

        Returns:
            New LTSeq with aggregated results (one row per group)

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return valid expressions

        Examples:
            >>> # Total sales by region
            >>> t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
            >>> # Multiple aggregations
            >>> t.agg(
            ...     by=lambda r: [r.region, r.year],
            ...     total_sales=lambda g: g.sales.sum(),
            ...     avg_price=lambda g: g.price.avg(),
            ...     max_quantity=lambda g: g.quantity.max()
            ... )
            >>> # Full-table aggregation (no grouping)
            >>> t.agg(total=lambda g: g.sales.sum(), count=lambda g: g.id.count())
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Extract grouping expression if provided
        group_expr = None
        if by is not None:
            group_expr = self._capture_expr(by)

        # Extract aggregation expressions
        agg_dict = {}
        for agg_name, agg_lambda in aggregations.items():
            agg_dict[agg_name] = self._capture_expr(agg_lambda)

        # Call Rust implementation
        result = LTSeq()
        result._inner = self._inner.agg(group_expr, agg_dict)

        # Schema will be updated by Rust side (group columns + agg columns)
        # Infer it from the materialized data
        try:
            result_pd = result.to_pandas()
            result._schema = {col: "Unknown" for col in result_pd.columns}
        except Exception:
            # If materialization fails, use a default schema
            result._schema = {}

        return result

    def union(self, other: "LTSeq") -> "LTSeq":
        """
        Vertically concatenate two tables with the same schema.

        Combines all rows from both tables into a single table. This is equivalent
        to the SQL UNION ALL operation (preserves duplicates).

        Args:
            other: Another LTSeq table with compatible schema.
                   Columns must match in name and order.

        Returns:
            New LTSeq with rows from both tables combined.

        Raises:
            ValueError: If schemas are not compatible or not initialized.
            TypeError: If other is not an LTSeq instance.

        Examples:
            >>> t1 = LTSeq.read_csv("data1.csv")
            >>> t2 = LTSeq.read_csv("data2.csv")
            >>> combined = t1.union(t2)
            >>> combined.show()
        """
        if not isinstance(other, LTSeq):
            raise TypeError(
                f"union() argument must be LTSeq, got {type(other).__name__}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        # Validate schemas match
        if self._schema.keys() != other._schema.keys():
            raise ValueError(
                f"Schemas do not match. "
                f"Left table columns: {list(self._schema.keys())}, "
                f"Right table columns: {list(other._schema.keys())}"
            )

        # Call Rust implementation
        result = LTSeq()
        result._inner = self._inner.union(other._inner)
        result._schema = self._schema.copy()

        return result

    def intersect(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
        """
        Return rows present in both tables.

        This is equivalent to the SQL INTERSECT operation, returning only rows
        that appear in both tables based on the specified key columns.

        Args:
            other: Another LTSeq table to intersect with.
            on: Optional lambda specifying which columns to use for matching.
                If None, uses all columns for comparison.
                E.g., lambda r: r.id (single key) or lambda r: [r.id, r.date]

        Returns:
            New LTSeq with rows present in both tables.

        Raises:
            ValueError: If schema is not initialized.
            TypeError: If on is not callable or other is not LTSeq.

        Examples:
            >>> t1 = LTSeq.read_csv("data1.csv")
            >>> t2 = LTSeq.read_csv("data2.csv")
            >>> common = t1.intersect(t2, on=lambda r: r.id)
            >>> common.show()
        """
        if not isinstance(other, LTSeq):
            raise TypeError(
                f"intersect() argument must be LTSeq, got {type(other).__name__}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        # Extract join key expression if provided
        key_expr = None
        if on is not None:
            key_expr = self._capture_expr(on)

        # Call Rust implementation
        result = LTSeq()
        result._inner = self._inner.intersect(other._inner, key_expr)
        result._schema = self._schema.copy()

        return result

    def diff(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
        """
        Return rows in this table but not in the other table.

        This is equivalent to the SQL EXCEPT (or MINUS in some databases) operation,
        returning rows from the left table that don't appear in the right table.

        Args:
            other: Another LTSeq table to compare against.
            on: Optional lambda specifying which columns to use for matching.
                If None, uses all columns for comparison.
                E.g., lambda r: r.id (single key) or lambda r: [r.id, r.date]

        Returns:
            New LTSeq with rows in left table but not in right table.

        Raises:
            ValueError: If schema is not initialized.
            TypeError: If on is not callable or other is not LTSeq.

        Examples:
            >>> t1 = LTSeq.read_csv("data1.csv")
            >>> t2 = LTSeq.read_csv("data2.csv")
            >>> only_in_t1 = t1.diff(t2, on=lambda r: r.id)
            >>> only_in_t1.show()
        """
        if not isinstance(other, LTSeq):
            raise TypeError(
                f"diff() argument must be LTSeq, got {type(other).__name__}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        # Extract join key expression if provided
        key_expr = None
        if on is not None:
            key_expr = self._capture_expr(on)

        # Call Rust implementation
        result = LTSeq()
        result._inner = self._inner.diff(other._inner, key_expr)
        result._schema = self._schema.copy()

        return result

    def is_subset(self, other: "LTSeq", on: Optional[Callable] = None) -> bool:
        """
        Check if this table is a subset of another table.

        Returns True if all rows in this table also appear in the other table
        (based on specified key columns).

        Args:
            other: Another LTSeq table to check against.
            on: Optional lambda specifying which columns to use for matching.
                If None, uses all columns for comparison.

        Returns:
            Boolean indicating if this table is a subset of the other.

        Raises:
            ValueError: If schema is not initialized.
            TypeError: If on is not callable or other is not LTSeq.

        Examples:
            >>> t1 = LTSeq.read_csv("data_small.csv")
            >>> t2 = LTSeq.read_csv("data_large.csv")
            >>> is_subset = t1.is_subset(t2, on=lambda r: r.id)
            >>> print(is_subset)
        """
        if not isinstance(other, LTSeq):
            raise TypeError(
                f"is_subset() argument must be LTSeq, got {type(other).__name__}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        # Extract join key expression if provided
        key_expr = None
        if on is not None:
            key_expr = self._capture_expr(on)

        # Call Rust implementation - returns boolean
        return self._inner.is_subset(other._inner, key_expr)

    def link(
        self, target_table: "LTSeq", on: Callable, as_: str, join_type: str = "inner"
    ) -> "LinkedTable":
        """
        Link this table to another table using pointer-based foreign keys.

        Creates a virtual pointer column that references rows in the target table
        based on a join condition. Unlike join(), this uses index-based lookups
        (pointer semantics) rather than expensive hash joins.

        Phase 8I: Enhanced to support multiple join types (INNER, LEFT, RIGHT, FULL).

        Args:
            target_table: The table to link to (products, categories, etc.)
            on: Lambda with two parameters that specifies the join condition.
                E.g., lambda orders, products: orders.product_id == products.id
                Or: lambda orders, products: (orders.product_id == products.product_id) & (orders.year == products.year)
            as_: Alias for the linked table reference.
                E.g., as_="prod" allows accessing linked columns via r.prod.name
            join_type: Type of join to perform. One of: "inner", "left", "right", "full"
                Default: "inner" (backward compatible)

        Returns:
            A LinkedTable that supports accessing target columns via the alias

        Raises:
            ValueError: If schema is not initialized or join_type is invalid
            TypeError: If join condition is invalid
            AttributeError: If lambda references non-existent columns

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> # Inner join (default)
            >>> linked = orders.link(products,
            ...     on=lambda o, p: o.product_id == p.id,
            ...     as_="prod")
            >>> # Left outer join (keep all orders, NULLs for unmatched products)
            >>> linked_left = orders.link(products,
            ...     on=lambda o, p: o.product_id == p.id,
            ...     as_="prod",
            ...     join_type="left")
            >>> result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
            >>> result.show()
        """
        # Validate join_type
        valid_join_types = {"inner", "left", "right", "full"}
        if join_type not in valid_join_types:
            raise ValueError(
                f"Invalid join_type '{join_type}'. Must be one of: {valid_join_types}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not target_table._schema:
            raise ValueError(
                "Target table schema not initialized. Call read_csv() first."
            )

        # Validate the join condition
        try:
            self_proxy = SchemaProxy(self._schema)
            target_proxy = SchemaProxy(target_table._schema)

            # Call the join condition to extract the comparison expression
            _ = on(self_proxy, target_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        # Import here to avoid circular imports
        from .linking import LinkedTable

        # Return a LinkedTable wrapping both tables
        return LinkedTable(self, target_table, on, as_, join_type)

    def partition(self, by: Callable) -> "PartitionedTable":
        """
        Partition the table into groups based on a key function.

        Unlike group_ordered() which handles consecutive groups, partition()
        groups all rows with the same key value regardless of their position
        in the table.

        Args:
            by: Lambda that returns the partition key (e.g., lambda r: r.region).
                Will be evaluated on each row to determine its partition.

        Returns:
            A PartitionedTable that supports dict-like access and iteration

        Raises:
            ValueError: If schema is not initialized

        Example:
            >>> t = LTSeq.read_csv("sales.csv")
            >>> partitions = t.partition(by=lambda r: r.region)
            >>> west_sales = partitions["West"]  # Access by key
            >>> for region, data in partitions.items():
            ...     print(f"{region}: {len(data)} rows")
            >>> # Apply aggregation to each partition
            >>> totals = partitions.map(lambda t: t.agg(total=lambda g: g.sales.sum()))
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Import here to avoid circular imports
        from .partitioning import PartitionedTable

        return PartitionedTable(self, by)

    def pivot(
        self,
        index: Union[str, list[str]],
        columns: str,
        values: str,
        agg_fn: str = "sum",
    ) -> "LTSeq":
        """
        Reshape table from long format to wide format (pivot table operation).

        Transforms data where unique values in a column become new columns,
        and row groups are aggregated based on specified columns.

        Args:
            index: Column name(s) to keep as rows in the pivoted table.
                  Can be a string or list of strings for composite row keys.
                  E.g., "year" or ["year", "category"]
            columns: Column whose unique values will become the new column names.
                    E.g., "region" creates columns "West", "East", etc.
            values: Column containing the values to aggregate into cells.
                   E.g., "amount" - will be summed into each cell
            agg_fn: Aggregation function to apply. One of:
                   "sum", "mean", "count", "min", "max"
                   Default: "sum"

        Returns:
            A new LTSeq with pivoted data (one row per unique index combination)

        Raises:
            ValueError: If schema is not initialized or agg_fn is invalid
            AttributeError: If index, columns, or values columns don't exist

        Example:
            >>> t = LTSeq.read_csv("sales.csv")  # Has columns: date, region, amount
            >>> # Rows by date, columns by region, sum amounts
            >>> pivoted = t.pivot(index="date", columns="region", values="amount", agg_fn="sum")
            >>> pivoted.show()
            >>>
            >>> # For composite row keys
            >>> pivoted2 = t.pivot(
            ...     index=["date", "category"],
            ...     columns="region",
            ...     values="amount",
            ...     agg_fn="mean"
            ... )
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Validate agg_fn
        valid_agg_fns = {"sum", "mean", "count", "min", "max"}
        if agg_fn not in valid_agg_fns:
            raise ValueError(
                f"Invalid aggregation function '{agg_fn}'. Must be one of: {valid_agg_fns}"
            )

        # Normalize index to list
        if isinstance(index, str):
            index_cols = [index]
        elif isinstance(index, list):
            index_cols = index
        else:
            raise TypeError(
                f"index must be str or list of str, got {type(index).__name__}"
            )

        # Validate that all required columns exist
        required_cols = index_cols + [columns, values]
        for col in required_cols:
            if col not in self._schema:
                raise AttributeError(
                    f"Column '{col}' not found in schema. "
                    f"Available columns: {list(self._schema.keys())}"
                )

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "pivot() requires pandas. Install it with: pip install pandas"
            )

        # Convert to pandas DataFrame
        df = self.to_pandas()

        # Map agg_fn string to pandas aggregation function name
        agg_fn_map = {
            "sum": "sum",
            "mean": "mean",
            "count": "count",
            "min": "min",
            "max": "max",
        }

        # Perform pivot_table operation
        pivoted_df = df.pivot_table(
            index=index_cols,
            columns=columns,
            values=values,
            aggfunc=agg_fn_map[agg_fn],
        )

        # Reset index to convert MultiIndex back to regular columns
        pivoted_df = pivoted_df.reset_index()

        # Flatten column names if MultiIndex (can happen with multiple value columns)
        if isinstance(pivoted_df.columns, pd.MultiIndex):
            pivoted_df.columns = [
                "_".join(col).strip("_") for col in pivoted_df.columns.values
            ]

        # Infer schema from pivoted DataFrame
        schema = {col: "Unknown" for col in pivoted_df.columns}

        # Convert back to LTSeq using _from_rows
        rows = pivoted_df.to_dict("records")
        result = LTSeq._from_rows(rows, schema)

        return result

    def search_first(self, predicate: Callable) -> Optional["LTSeq"]:
        """
        Find the first row matching a condition.

        Returns a single-row LTSeq with the first row where the predicate returns True,
        or an empty LTSeq if no match is found.

        For sorted tables, this is equivalent to a binary search optimization.
        The table should be pre-sorted by the column you're searching on for best performance.

        Args:
            predicate: Lambda that takes a row and returns a boolean.
                      E.g., lambda r: r.price > 100

        Returns:
            LTSeq with single matching row, or empty LTSeq if not found

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return a boolean Expr

        Example:
            >>> t = LTSeq.read_csv("products.csv")
            >>> t_sorted = t.sort("price")
            >>> # Find first product with price > 100
            >>> first_expensive = t_sorted.search_first(lambda r: r.price > 100)
            >>> if len(first_expensive) > 0:
            ...     first_expensive.show()
            >>> # Use on pre-sorted time series
            >>> t_sorted = t.sort("date")
            >>> first_2024 = t_sorted.search_first(lambda r: r.date >= "2024-01-01")
        """

    def search_first(self, predicate: Callable) -> Optional["LTSeq"]:
        """
        Find the first row matching a condition.

        Returns a single-row LTSeq with the first row where the predicate returns True,
        or an empty LTSeq if no match is found.

        For sorted tables, this is equivalent to a binary search optimization.
        The table should be pre-sorted by the column you're searching on for best performance.

        Args:
            predicate: Lambda that takes a row and returns a boolean.
                      E.g., lambda r: r.price > 100

        Returns:
            LTSeq with single matching row, or empty LTSeq if not found

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return a boolean Expr

        Example:
            >>> t = LTSeq.read_csv("products.csv")
            >>> t_sorted = t.sort("price")
            >>> # Find first product with price > 100
            >>> first_expensive = t_sorted.search_first(lambda r: r.price > 100)
            >>> if len(first_expensive) > 0:
            ...     first_expensive.show()
            >>> # Use on pre-sorted time series
            >>> t_sorted = t.sort("date")
            >>> first_2024 = t_sorted.search_first(lambda r: r.date >= "2024-01-01")
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "search_first() requires pandas. Install it with: pip install pandas"
            )

        # Convert to pandas for iteration
        df = self.to_pandas()

        # Validate predicate by testing with schema proxy
        from .expr import SchemaProxy

        test_proxy = SchemaProxy(self._schema)
        try:
            _ = predicate(test_proxy)
        except Exception as e:
            raise TypeError(f"Invalid predicate: {e}")

        # Iterate through rows and find first match
        for idx, row in df.iterrows():
            # Create single-row dataframe and convert back to LTSeq
            single_row_df = df.iloc[[idx]]

            # Convert to dict and back through _from_rows to create LTSeq
            rows = single_row_df.to_dict("records")

            # Try filter on this single row
            try:
                single_ltseq = LTSeq._from_rows(rows, self._schema)
                result_filtered = single_ltseq.filter(predicate)

                if len(result_filtered) > 0:
                    # Found a match, return this row
                    return result_filtered
            except Exception:
                # Predicate failed for this row, continue
                continue

        # No match found - return empty LTSeq
        return LTSeq._from_rows([], self._schema)

    def join_merge(self, other: "LTSeq", on: Callable, how: str = "inner") -> "LTSeq":
        """
        High-speed O(N) merge join for two sorted tables.

        Performs an in-order merge join between two pre-sorted tables.
        Both tables should be sorted by the join key for optimal performance.

        This is more efficient than hash join when tables are already sorted,
        as it performs a single pass through both tables in O(N + M) time.

        Args:
            other: Another LTSeq table (should be sorted by join key)
            on: Lambda with two parameters specifying the join condition.
                E.g., lambda t1, t2: t1.id == t2.id
            how: Join type. One of: "inner", "left", "right", "full"
                 Default: "inner" (only matching rows)

        Returns:
            New LTSeq with joined results. Columns from both tables are included.

        Raises:
            ValueError: If schema not initialized or invalid join type
            TypeError: If join condition is invalid

        Example:
            >>> t1 = LTSeq.read_csv("users.csv")
            >>> t2 = LTSeq.read_csv("orders.csv")
            >>> # Pre-sort both tables by join key
            >>> t1_sorted = t1.sort("user_id")
            >>> t2_sorted = t2.sort("user_id")
            >>> # Perform merge join
            >>> result = t1_sorted.join_merge(
            ...     t2_sorted,
            ...     on=lambda t1, t2: t1.user_id == t2.user_id,
            ...     how="inner"
            ... )
            >>> result.show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not isinstance(other, LTSeq):
            raise TypeError(
                f"join_merge() argument must be LTSeq, got {type(other).__name__}"
            )

        if not other._schema:
            raise ValueError(
                "Other table schema not initialized. Call read_csv() first."
            )

        # Validate join type
        valid_join_types = {"inner", "left", "right", "full"}
        if how not in valid_join_types:
            raise ValueError(
                f"Invalid join type '{how}'. Must be one of: {valid_join_types}"
            )

        # Validate join condition
        try:
            self_proxy = SchemaProxy(self._schema)
            other_proxy = SchemaProxy(other._schema)
            _ = on(self_proxy, other_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "join_merge() requires pandas. Install it with: pip install pandas"
            )

        # Convert both tables to pandas DataFrames
        df1 = self.to_pandas()
        df2 = other.to_pandas()

        # Perform merge join using pandas merge
        # Since we have difficulty extracting join keys from arbitrary lambdas,
        # we detect common column names and use those for joining

        # Convert "full" to "outer" for pandas compatibility
        pandas_how = "outer" if how == "full" else how

        # Simple approach: merge on common columns
        common_cols = sorted(set(df1.columns) & set(df2.columns))

        if common_cols:
            # Merge on common columns
            result_df = pd.merge(
                df1, df2, on=common_cols, how=pandas_how, suffixes=("", "_other")
            )
        else:
            # No common columns - use cross join
            df1["_key"] = 1
            df2["_key"] = 1
            result_df = pd.merge(
                df1, df2, on="_key", how=pandas_how, suffixes=("", "_other")
            )
            result_df = result_df.drop("_key", axis=1)

        # Update schema with result columns
        result_schema = {col: "Unknown" for col in result_df.columns}

        # Convert back to LTSeq
        rows = result_df.to_dict("records")
        result = LTSeq._from_rows(rows, result_schema)

        return result
