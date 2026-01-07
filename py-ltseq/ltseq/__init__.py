# LTSeq Python wrapper — uses native `ltseq_core` extension for CSV IO and operations.

from typing import Callable, Dict, Any, Optional, Union
import csv

# Import the expression system early
from .expr import _lambda_to_expr, SchemaProxy, BinOpExpr, ColumnExpr

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


def _extract_join_keys(
    join_fn: Callable, source_schema: Dict[str, str], target_schema: Dict[str, str]
) -> tuple:
    """
    Extract join key columns from a two-parameter lambda expression.

    Phase 8A: Parses join conditions like:
        lambda orders, products: orders.product_id == products.id

    Phase 8H: Enhanced to support composite keys:
        lambda orders, products: (orders.product_id == products.id) & (orders.year == products.year)

    to extract:
        left_key="product_id", right_key="id", join_type="inner"
        OR for composite:
        left_key=(expr1 & expr2), right_key=(expr1 & expr2), join_type="inner"

    The function validates that:
    1. The lambda returns a BinOpExpr with op="Eq" or "And" (for composite)
    2. All operands are equality comparisons between columns
    3. Left operands are from source table, right from target
    4. All columns exist in their respective schemas

    Args:
        join_fn: Lambda function with two parameters (source row, target row)
                E.g., lambda o, p: o.product_id == p.id
                Or: lambda o, p: (o.id == p.id) & (o.year == p.year)
        source_schema: Dict mapping source column names to types
        target_schema: Dict mapping target column names to types

    Returns:
        Tuple of (left_key_expr, right_key_expr, join_type) where:
        - left_key_expr: Serialized expression for left join key(s)
        - right_key_expr: Serialized expression for right join key(s)
        - join_type: String "inner" (only supported for MVP)

    Raises:
        TypeError: If join condition is not equality comparisons
        ValueError: If columns don't exist in respective schemas
        AttributeError: If lambda references non-existent columns

    Example:
        >>> orders_schema = {"id": "int64", "product_id": "int64", "year": "int64"}
        >>> products_schema = {"id": "int64", "product_id": "int64", "year": "int64", "name": "string"}
        >>> # Single key
        >>> left_key, right_key, join_type = _extract_join_keys(
        ...     lambda o, p: o.product_id == p.product_id,
        ...     orders_schema,
        ...     products_schema
        ... )
        >>> # Composite key (Phase 8H)
        >>> left_key, right_key, join_type = _extract_join_keys(
        ...     lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
        ...     orders_schema,
        ...     products_schema
        ... )
    """
    # Create proxies for both tables
    source_proxy = SchemaProxy(source_schema)
    target_proxy = SchemaProxy(target_schema)

    # Call the join function to get the expression tree
    try:
        expr = join_fn(source_proxy, target_proxy)
    except Exception as e:
        raise TypeError(f"Failed to evaluate join condition: {e}")

    # Validate and extract key expressions
    # Handle both single equality and composite And-expressions
    if not isinstance(expr, BinOpExpr):
        raise TypeError(
            f"Join condition must be a simple equality comparison or composite And-expression, got {type(expr).__name__}"
        )

    # Phase 8H: Handle composite keys with And operator
    if expr.op == "And":
        # Recursively extract all equality pairs from And-expression
        # Structure: (a == b) & (c == d) is represented as nested BinOp with op="And"
        all_equalities = _extract_all_equalities_from_and(expr)

        if not all_equalities:
            raise TypeError(
                "And-expression must contain at least one equality comparison"
            )

        # Validate all equalities
        for left_eq, right_eq in all_equalities:
            _validate_equality_pair(left_eq, right_eq, source_schema, target_schema)

        # For Rust compatibility, we keep single key expressions but they're And-composed
        # Return the original And-expression as-is
        left_key_expr = expr.serialize()
        right_key_expr = expr.serialize()
        return left_key_expr, right_key_expr, "inner"

    # Phase 8A: Handle single equality
    elif expr.op == "Eq":
        # Extract left and right operands
        left_expr = expr.left
        right_expr = expr.right

        # Validate operands are ColumnExpr
        if not isinstance(left_expr, ColumnExpr) or not isinstance(
            right_expr, ColumnExpr
        ):
            raise TypeError(
                "Join condition must compare two columns directly. "
                "Complex expressions are not supported in Phase 8."
            )

        # Validate the pair
        _validate_equality_pair(left_expr, right_expr, source_schema, target_schema)

        # Serialize the key expressions
        left_key_expr = left_expr.serialize()
        right_key_expr = right_expr.serialize()

        # Return as tuple (MVP only supports inner join)
        return left_key_expr, right_key_expr, "inner"

    else:
        raise TypeError(
            f"Join condition must use equality (==), got operator: {expr.op}. "
            "Only inner equality joins are supported in Phase 8."
        )


def _extract_all_equalities_from_and(expr: BinOpExpr) -> list:
    """
    Recursively extract all equality pairs from an And-expression tree.

    Handles nested And-expressions like: (a==b) & ((c==d) & (e==f))

    Args:
        expr: BinOpExpr with op="And"

    Returns:
        List of tuples: [(left_col_expr, right_col_expr), ...]
    """
    equalities = []

    # Recursively traverse the And-expression
    if isinstance(expr.left, BinOpExpr):
        if expr.left.op == "And":
            # Recurse on left side if it's also an And
            equalities.extend(_extract_all_equalities_from_and(expr.left))
        elif expr.left.op == "Eq":
            # Found an equality on the left
            equalities.append((expr.left.left, expr.left.right))

    if isinstance(expr.right, BinOpExpr):
        if expr.right.op == "And":
            # Recurse on right side if it's also an And
            equalities.extend(_extract_all_equalities_from_and(expr.right))
        elif expr.right.op == "Eq":
            # Found an equality on the right
            equalities.append((expr.right.left, expr.right.right))

    return equalities


def _validate_equality_pair(
    left_expr: Any,
    right_expr: Any,
    source_schema: Dict[str, str],
    target_schema: Dict[str, str],
) -> None:
    """
    Validate that a left-right pair are both ColumnExprs from correct tables.

    Args:
        left_expr: Expression from left side of equality
        right_expr: Expression from right side of equality
        source_schema: Source table schema
        target_schema: Target table schema

    Raises:
        TypeError: If not both ColumnExpr
        ValueError: If columns aren't in correct tables
    """
    # Validate operands are ColumnExpr
    if not isinstance(left_expr, ColumnExpr) or not isinstance(right_expr, ColumnExpr):
        raise TypeError(
            "Join condition must compare two columns directly. "
            "Complex expressions are not supported in Phase 8."
        )

    # Validate that left operand is from source and right from target
    left_in_source = left_expr.name in source_schema
    left_in_target = left_expr.name in target_schema
    right_in_source = right_expr.name in source_schema
    right_in_target = right_expr.name in target_schema

    # Left must be in source, right must be in target
    if not left_in_source or not right_in_target:
        # Check if they're reversed
        if left_in_target and right_in_source:
            raise ValueError(
                f"Join condition has reversed order. Expected: source.col == target.col, "
                f"but got: {left_expr.name} == {right_expr.name}. "
                f"Please reverse the comparison: target.{left_expr.name} == source.{right_expr.name}"
            )
        # Otherwise, columns don't exist in expected schemas
        if not left_in_source:
            raise ValueError(
                f"Column '{left_expr.name}' not found in source schema. "
                f"Available columns: {list(source_schema.keys())}"
            )
        if not right_in_target:
            raise ValueError(
                f"Column '{right_expr.name}' not found in target schema. "
                f"Available columns: {list(target_schema.keys())}"
            )


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

        # Handle both API styles
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
            expr_dict = self._capture_expr(func)

            # The captured expression should return a Call with type "return" or be a BinOp/etc
            # We need to extract the column definitions from it
            # For now, assume it's a dict literal that we can extract
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
                        raise ValueError(
                            "Dict keys in derive() must be string literals"
                        )
            else:
                # Fallback: treat the entire expression as a single derived column
                # This shouldn't happen for well-formed calls, but we provide a default
                derived_cols = {"_derived": expr_dict}
        else:
            # API 1: keyword arguments
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
            # Phase 6.1: Window functions are now handled by Rust backend
            # They will be transpiled to SQL and executed by DataFusion
            result._inner = self._inner.derive(derived_cols)
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

        # Return a NestedTable wrapping this LTSeq
        return NestedTable(self, grouping_fn)

    def link(self, target_table: "LTSeq", on: Callable, as_: str) -> "LinkedTable":
        """
        Link this table to another table using pointer-based foreign keys.

        Creates a virtual pointer column that references rows in the target table
        based on a join condition. Unlike join(), this uses index-based lookups
        (pointer semantics) rather than expensive hash joins.

        Args:
            target_table: The table to link to (products, categories, etc.)
            on: Lambda with two parameters that specifies the join condition.
                E.g., lambda orders, products: orders.product_id == products.id
            as_: Alias for the linked table reference.
                E.g., as_="prod" allows accessing linked columns via r.prod.name

        Returns:
            A LinkedTable that supports accessing target columns via the alias

        Raises:
            ValueError: If schema is not initialized
            TypeError: If join condition is invalid
            AttributeError: If lambda references non-existent columns

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> linked = orders.link(products,
            ...     on=lambda o, p: o.product_id == p.id,
            ...     as_="prod")
            >>> result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
            >>> result.show()
        """
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

        # Return a LinkedTable wrapping both tables
        return LinkedTable(self, target_table, on, as_)


__all__ = ["LTSeq", "NestedTable", "LinkedTable"]


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

        # Add internal group_id column to schema
        self._schema["__group_id__"] = "int64"

    def first(self) -> "ColumnExpr":
        """
        Get the first row of each group.

        Returns a special ColumnExpr that represents the first row within each group.

        Example:
            >>> grouped.first().price  # First price in each group
        """
        from .expr import CallExpr, ColumnExpr

        # Create a special marker that will be handled by Rust
        return CallExpr("__first__", (), {}, on=None)

    def last(self) -> "ColumnExpr":
        """
        Get the last row of each group.

        Returns a special ColumnExpr that represents the last row within each group.

        Example:
            >>> grouped.last().price  # Last price in each group
        """
        from .expr import CallExpr, ColumnExpr

        return CallExpr("__last__", (), {}, on=None)

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
        Return the underlying LTSeq with group_id column added.

        Useful for debugging or accessing raw grouped data.
        """
        # For now, return the underlying ltseq
        # In a full implementation, this would add the __group_id__ column
        return self._ltseq

    def filter(self, group_predicate: Callable) -> "NestedTable":
        """
        Filter groups based on a predicate on group properties.

        Args:
            group_predicate: Lambda that takes a group and returns boolean.
                           Can use g.count(), g.first(), g.last()

        Returns:
            A new NestedTable with filtered groups

        Example:
            >>> grouped.filter(lambda g: g.count() > 3)
        """
        try:
            # Create a new NestedTable that combines both predicates
            original_grouping = self._grouping_lambda

            def combined_filter(r):
                # First check the grouping condition
                grouping_key = original_grouping(r)
                # Then check the group predicate (simplified for now)
                return grouping_key

            result = NestedTable(self._ltseq, combined_filter)
            result._group_filter = group_predicate
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to filter groups: {e}")

    def derive(self, group_mapper: Callable) -> "NestedTable":
        """
        Derive new columns based on group properties.

        Args:
        group_mapper: Lambda that returns a dict of new columns based on group.
        Can use g.first(), g.last(), g.count()

        Returns:
        A new NestedTable with derived columns

        Example:
        >>> grouped.derive(lambda g: {"span": g.count(), "gain": g.last().price - g.first().price})
        """
        try:
            result = NestedTable(self._ltseq, self._grouping_lambda)
            result._group_derive = group_mapper
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to derive group columns: {e}")


class LinkedTable:
    """
    Represents a table with pointer-based foreign key references.

    Created by LTSeq.link(), this wrapper maintains pointers to rows in a target
    table without materializing an expensive join. Accessing linked columns is
    translated to index-based lookups (take operations) during execution.

    MVP Phase 8: Basic pointer table support with schema awareness.
    """

    def __init__(
        self,
        source_table: "LTSeq",
        target_table: "LTSeq",
        join_fn: Callable,
        alias: str,
    ):
        """
        Initialize a LinkedTable from source and target tables.

        Args:
            source_table: The primary table (e.g., orders)
            target_table: The table being linked (e.g., products)
            join_fn: Lambda that specifies the join condition
            alias: Alias used for the linked reference (e.g., "prod")
        """
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        self._schema = source_table._schema.copy()
        self._materialized: Optional["LTSeq"] = None  # Phase 8B: Lazy materialization

        # Add linked column metadata to schema with prefix
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type

    def _materialize(self) -> "LTSeq":
        """
        Phase 8B: Materialize the join operation.

        This method performs the actual join only when data access is needed.
        The result is cached to avoid re-joining on subsequent accesses.

        Returns:
            LTSeq instance with materialized joined data

        Raises:
            TypeError: If join condition cannot be parsed
            ValueError: If join columns don't exist
        """
        # Return cached result if already materialized
        if self._materialized is not None:
            return self._materialized

        # Extract join keys using Phase 8A function
        left_key_expr, right_key_expr, join_type = _extract_join_keys(
            self._join_fn,
            self._source._schema,
            self._target._schema,
        )

        # Call Rust join() method - it handles all schema conflicts and column renaming
        joined_inner = self._source._inner.join(
            self._target._inner,
            left_key_expr,
            right_key_expr,
            join_type,
            self._alias,
        )

        # Create new LTSeq wrapping the joined result
        result = LTSeq()
        result._inner = joined_inner
        result._schema = self._schema.copy()

        # Cache the result
        self._materialized = result
        return result

    def show(self, n: int = 10) -> None:
        """Display linked table with materialized joined data."""
        materialized = self._materialize()
        materialized.show(n)

    def filter(self, predicate: Callable) -> Union["LinkedTable", "LTSeq"]:
        """
        Filter rows with support for both source and linked columns.

        Phase 8F: Enhanced to materialize join if predicate references linked columns.

        If predicate uses only source columns, returns a LinkedTable with filtered source.
        If predicate uses linked columns, materializes the join and returns filtered LTSeq.

        Args:
            predicate: Lambda taking a row, returning boolean Expr
                      Can reference source columns (id, quantity) OR linked columns (prod.name, prod.price)

        Returns:
            LinkedTable if filtering on source columns only
            LTSeq if filtering on linked columns (join materialized)

        Raises:
            AttributeError: If predicate references non-existent columns
        """
        # Try to parse with source schema only
        try:
            _lambda_to_expr(predicate, self._source._schema)
            # Predicate only uses source columns - fast path
            filtered_source = self._source.filter(predicate)
            return LinkedTable(
                filtered_source, self._target, self._join_fn, self._alias
            )
        except AttributeError:
            # Predicate references columns not in source
            # These must be linked columns - materialize and filter
            materialized = self._materialize()
            return materialized.filter(predicate)

    def select(self, *cols) -> "LTSeq":
        """
        Select columns from linked table.

        Phase 8G: Enhanced to materialize join if selecting linked columns.
        Currently only source columns selection works reliably; selecting linked columns
        (with alias prefix like 'prod_*') requires accessing the materialized table directly.

        Args:
            *cols: Column names to select from source table (e.g., "id", "quantity")
                   Linked columns (e.g., "prod_name") require manual materialization

        Returns:
            LTSeq with selected columns from source

        Example:
            >>> linked = orders.link(products, on=lambda o,p: o.product_id==p.product_id, as_="prod")
            >>> result = linked.select("id", "quantity")  # ✓ Works: source columns
            >>> # For linked columns, use: linked._materialize().select(...)
        """
        # Note: Current implementation materializes the join but select() on materialized
        # tables doesn't work with linked column names due to DataFrame column name mismatch.
        # This is planned for Phase 8G enhancement.
        materialized = self._materialize()
        return materialized.select(*cols)

    def derive(self, mapper: Callable) -> "LinkedTable":
        """Derive columns (delegates to source for MVP)."""
        derived_source = self._source.derive(mapper)
        return LinkedTable(derived_source, self._target, self._join_fn, self._alias)

    def slice(self, start: int, end: int) -> "LinkedTable":
        """Slice rows."""
        sliced_source = self._source.slice(start, end)
        return LinkedTable(sliced_source, self._target, self._join_fn, self._alias)

    def distinct(self, key_fn: Callable = None) -> "LinkedTable":
        """Get distinct rows."""
        if key_fn is None:
            distinct_source = self._source.distinct()
        else:
            distinct_source = self._source.distinct(key_fn)
        return LinkedTable(distinct_source, self._target, self._join_fn, self._alias)

    def link(self, target_table: "LTSeq", on: Callable, as_: str) -> "LinkedTable":
        """
        Link this linked table to another table.

        Allows chaining multiple links while preserving previous schemas.
        """
        # Create new LinkedTable that chains from the source
        result = LinkedTable(self._source, target_table, on, as_)

        # Merge with existing schema from this LinkedTable
        for key, value in self._schema.items():
            if key not in result._schema:
                result._schema[key] = value

        return result
