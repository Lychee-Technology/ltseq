# LTSeq Python wrapper — uses native `ltseq_core` extension for CSV IO and operations.

from typing import Callable, Dict, Any, Optional, Union
import csv

# Import the expression system early
from .expr import _lambda_to_expr, SchemaProxy

# Import the compiled Rust module
try:
    from . import ltseq_core

    HAS_RUST_BINDING = True
except ImportError:
    HAS_RUST_BINDING = False


def _eval_expr_on_row(expr_dict: Dict[str, Any], row_proxy: SchemaProxy) -> Any:
    """
    Evaluate a serialized expression tree on a given row.

    Args:
        expr_dict: Serialized expression dict
        row_proxy: SchemaProxy instance bound to current row

    Returns:
        The result of evaluating the expression
    """
    expr_type = expr_dict.get("type")

    if expr_type == "Column":
        col_name = expr_dict["name"]
        return row_proxy[col_name]

    elif expr_type == "Literal":
        return expr_dict["value"]

    elif expr_type == "BinOp":
        op = expr_dict["op"]
        left = _eval_expr_on_row(expr_dict["left"], row_proxy)
        right = _eval_expr_on_row(expr_dict["right"], row_proxy)

        if op == "Add":
            return left + right
        elif op == "Sub":
            return left - right
        elif op == "Mul":
            return left * right
        elif op == "Div":
            return left / right if right != 0 else None
        elif op == "Mod":
            return left % right if right != 0 else None
        elif op == "Eq":
            return left == right
        elif op == "Ne":
            return left != right
        elif op == "Lt":
            return left < right
        elif op == "Le":
            return left <= right
        elif op == "Gt":
            return left > right
        elif op == "Ge":
            return left >= right
        elif op == "And":
            return left and right
        elif op == "Or":
            return left or right
        else:
            raise ValueError(f"Unknown binary operator: {op}")

    elif expr_type == "UnaryOp":
        op = expr_dict["op"]
        operand = _eval_expr_on_row(expr_dict["operand"], row_proxy)

        if op == "Not":
            return not operand
        else:
            raise ValueError(f"Unknown unary operator: {op}")

    else:
        raise ValueError(f"Unknown expression type: {expr_type}")


class _RowEvaluator:
    """Helper to evaluate expressions on actual row data."""

    def __init__(self, row_data: Dict[str, Any]):
        self._row = row_data

    def __getitem__(self, key: str) -> Any:
        return self._row.get(key)


def _normalize_schema(schema_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Normalize DataFusion type strings to simplified format.

    Converts DataFusion's debug format (e.g., "Utf8", "Int64") to standard names.

    Args:
        schema_dict: Dict mapping column names to DataFusion type strings

    Returns:
        Dict mapping column names to normalized type strings
    """
    # Map common DataFusion types to normalized names
    type_map = {
        "Utf8": "string",
        "String": "string",
        "Int8": "int8",
        "Int16": "int16",
        "Int32": "int32",
        "Int64": "int64",
        "UInt8": "uint8",
        "UInt16": "uint16",
        "UInt32": "uint32",
        "UInt64": "uint64",
        "Float32": "float32",
        "Float64": "float64",
        "Boolean": "bool",
        "Bool": "bool",
    }

    normalized = {}
    for col_name, type_str in schema_dict.items():
        # Try to find normalized type, otherwise keep original
        normalized[col_name] = type_map.get(type_str.strip(), type_str)

    return normalized


def _infer_schema_from_csv(path: str) -> Dict[str, str]:
    """
    Infer schema from CSV file by reading the header and sampling rows.

    For Phase 2, this is a workaround until RustTable properly exposes schema.
    Infers type as:
    - "int64" if all values are integers
    - "float64" if all values are floats
    - "string" otherwise

    Args:
        path: Path to CSV file

    Returns:
        Dict mapping column names to inferred type strings
    """
    schema = {}

    try:
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return schema

            # Initialize schema with all columns as string
            for col in reader.fieldnames:
                schema[col] = "string"

            # Sample rows to infer types
            for i, row in enumerate(reader):
                if i >= 100:  # Sample first 100 rows
                    break

                for col, value in row.items():
                    if value == "":
                        continue

                    # Try to infer type
                    current_type = schema[col]

                    if current_type == "string":
                        continue

                    try:
                        int(value)
                        if current_type != "int64":
                            schema[col] = "int64"
                    except ValueError:
                        try:
                            float(value)
                            schema[col] = "float64"
                        except ValueError:
                            schema[col] = "string"

            return schema
    except Exception:
        # If anything goes wrong, return empty schema
        return {}


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
        # Call Rust RustTable.read_csv to load the actual data
        t._inner.read_csv(path)
        return t

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

    def hello(self) -> str:
        return self._inner.hello()

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
            if isinstance(col, str):
                # Column name reference
                if col not in self._schema:
                    raise AttributeError(
                        f"Column '{col}' not found in schema. "
                        f"Available columns: {list(self._schema.keys())}"
                    )
                exprs.append({"type": "Column", "name": col})
            elif callable(col):
                # Lambda that computes expressions or columns
                proxy = SchemaProxy(self._schema)
                result = col(proxy)

                # Check if result is a list (multiple column selection)
                if isinstance(result, list):
                    # Extract expressions from list
                    for item in result:
                        if hasattr(item, "serialize"):  # It's an Expr
                            exprs.append(item.serialize())
                        else:
                            raise TypeError(
                                f"List items must be column expressions, got {type(item)}"
                            )
                elif hasattr(result, "serialize"):
                    # Single expression result
                    exprs.append(result.serialize())
                else:
                    raise TypeError(
                        f"Lambda must return Expr(s) or list of Exprs, got {type(result)}"
                    )
            else:
                raise TypeError(
                    f"select() argument must be str or callable, got {type(col).__name__}"
                )

        # Create a new LTSeq with selected columns
        result = LTSeq()
        # Update schema based on selected columns
        # For now, we keep the full schema (proper schema reduction requires Rust support)
        result._schema = self._schema.copy()
        # Delegate selection to Rust implementation
        result._inner = self._inner.select(exprs)
        return result

    def derive(self, **kwargs: Callable) -> "LTSeq":
        """
        Create new derived columns based on lambda expressions.

        New columns are added to the dataframe; existing columns remain.

        Args:
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
            >>> derived = t.derive(total=lambda r: r.price * r.qty)
            >>> derived.show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        derived_cols = {}
        for col_name, fn in kwargs.items():
            if not callable(fn):
                raise TypeError(
                    f"derive() argument '{col_name}' must be callable, got {type(fn).__name__}"
                )
            derived_cols[col_name] = self._capture_expr(fn)

        # Create a new LTSeq with derived columns
        result = LTSeq()
        # Update schema: add derived columns (assuming Int64 for now as placeholder)
        result._schema = self._schema.copy()
        for col_name in derived_cols:
            # In Phase 2, we'd infer the dtype from the expression
            result._schema[col_name] = "Unknown"

        # Check if any expressions contain window functions
        has_window_functions = any(
            self._contains_window_function(expr) for expr in derived_cols.values()
        )

        if has_window_functions:
            # Phase 6: Handle window functions - for now, raise informative error
            raise NotImplementedError(
                "Window functions (shift, rolling, diff, cum_sum) are not yet fully implemented. "
                "This is a Phase 6 feature that requires DataFrame-level support. "
                "Current status: Infrastructure in place, implementation in progress."
            )
        else:
            # Phase 4/5: Standard Rust-based derivation
            result._inner = self._inner.derive(derived_cols)

        return result

    def _contains_window_function(self, expr_dict: Dict[str, Any]) -> bool:
        """
        Check if an expression tree contains window function calls.

        Window functions include: shift, rolling, diff, cum_sum, etc.
        """
        if expr_dict.get("type") == "Call":
            func_name = expr_dict.get("func", "")
            if func_name in ("shift", "rolling", "diff", "cum_sum"):
                return True

        # Recurse into sub-expressions
        if expr_dict.get("type") == "BinOp":
            return self._contains_window_function(
                expr_dict["left"]
            ) or self._contains_window_function(expr_dict["right"])
        elif expr_dict.get("type") == "UnaryOp":
            return self._contains_window_function(expr_dict["operand"])
        elif expr_dict.get("type") == "Call":
            # Check arguments
            for arg in expr_dict.get("args", []):
                if self._contains_window_function(arg):
                    return True
            for val in expr_dict.get("kwargs", {}).values():
                if self._contains_window_function(val):
                    return True
            # Check nested call
            if expr_dict.get("on"):
                if self._contains_window_function(expr_dict["on"]):
                    return True

        return False

    def sort(self, *key_exprs: Union[str, Callable]) -> "LTSeq":
        """
        Sort rows by one or more key expressions in ascending order.

        Reorders data based on column values. Multiple sort keys are applied in order.

        Args:
            *key_exprs: Column names (str) or lambda expressions that return sortable values.
                       Examples:
                       - sort("date") - sort by date column ascending
                       - sort(lambda r: r.date) - same as above
                       - sort("date", "id") - multi-key sort: first by date, then by id

        Returns:
            A new LTSeq with sorted data

        Raises:
            ValueError: If schema is not initialized
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.sort("name").show()
            >>> t.sort(lambda r: r.date, lambda r: r.id).show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        # Collect sort expressions
        sort_exprs = []
        for key_expr in key_exprs:
            if isinstance(key_expr, str):
                # String column name: create simple Column expression
                sort_exprs.append(
                    {
                        "type": "Column",
                        "name": key_expr,
                    }
                )
            elif callable(key_expr):
                # Lambda expression: capture and serialize
                expr_dict = self._capture_expr(key_expr)
                sort_exprs.append(expr_dict)
            else:
                raise TypeError(
                    f"sort() argument must be str or callable, got {type(key_expr).__name__}"
                )

        # Create a new LTSeq with sorted result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate sorting to Rust implementation
        result._inner = self._inner.sort(sort_exprs)
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
        key_cols = []
        for key_expr in key_exprs:
            if isinstance(key_expr, str):
                # String column name
                key_cols.append(
                    {
                        "type": "Column",
                        "name": key_expr,
                    }
                )
            elif callable(key_expr):
                # Lambda expression
                expr_dict = self._capture_expr(key_expr)
                key_cols.append(expr_dict)
            else:
                raise TypeError(
                    f"distinct() argument must be str or callable, got {type(key_expr).__name__}"
                )

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
        cum_exprs = []
        for col_expr in cols:
            if isinstance(col_expr, str):
                # String column name: create simple Column expression
                cum_exprs.append(
                    {
                        "type": "Column",
                        "name": col_expr,
                    }
                )
            elif callable(col_expr):
                # Lambda expression: capture and serialize
                expr_dict = self._capture_expr(col_expr)
                cum_exprs.append(expr_dict)
            else:
                raise TypeError(
                    f"cum_sum() argument must be str or callable, got {type(col_expr).__name__}"
                )

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


__all__ = ["LTSeq"]
