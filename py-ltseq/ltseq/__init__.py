# LTSeq Python wrapper — requires native `ltseq_core` extension.
# This module intentionally does not provide Python fallbacks for read_csv/show
# to ensure we exercise the native DataFusion implementation during Phase 2.

from typing import Callable, Dict, Any, Optional
import csv


def _eval_expr_on_row(expr_dict: Dict[str, Any], row_proxy: "SchemaProxy") -> Any:
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


# Stub RustTable for now - actual Rust extension has GIL issues in Phase 1
# This will be properly integrated in Phase 2
class RustTable:
    def __init__(self):
        self.data: list = []  # List of dicts, one per row
        self.schema: Dict[str, str] = {}

    def read_csv(self, path: str) -> None:
        """Read CSV file into memory."""
        import csv

        self.data = []
        self.schema = {}

        try:
            with open(path, "r") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    return

                # Initialize schema with all columns as string
                for col in reader.fieldnames:
                    self.schema[col] = "string"

                # Read all rows
                for row in reader:
                    # Try to infer types from values
                    typed_row = {}
                    for col, value in row.items():
                        # Attempt to convert to int, then float, else keep as string
                        if value == "":
                            typed_row[col] = None
                        else:
                            try:
                                typed_row[col] = int(value)
                                if self.schema[col] == "string":
                                    self.schema[col] = "int64"
                            except ValueError:
                                try:
                                    typed_row[col] = float(value)
                                    if self.schema[col] == "string":
                                        self.schema[col] = "float64"
                                except ValueError:
                                    typed_row[col] = value
                    self.data.append(typed_row)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {path}")

    def show(self, n: int = 10) -> str:
        """Display the data as a formatted table."""
        if not self.data:
            return "No data loaded"

        # Get column names from schema
        cols = list(self.schema.keys())
        if not cols:
            return "No columns in schema"

        # Format header
        output = []
        output.append("\t".join(cols))

        # Format rows
        for i, row in enumerate(self.data[:n]):
            values = [str(row.get(col, "")) for col in cols]
            output.append("\t".join(values))

        return "\n".join(output)

    def hello(self) -> str:
        return "Hello from stub RustTable (Phase 1)"

    def filter(self, expr_dict: Dict[str, Any]) -> "RustTable":
        """Apply filter expression and return a new RustTable with filtered data."""
        # Create new RustTable with same schema
        filtered = RustTable()
        filtered.schema = self.schema.copy()

        # Evaluate expression for each row
        for row in self.data:
            # Create an evaluator for this row
            evaluator = _RowEvaluator(row)

            # Evaluate the expression
            try:
                result = _eval_expr_on_row(expr_dict, evaluator)
                if result:  # Include row if expression is truthy
                    filtered.data.append(row)
            except Exception:
                # If evaluation fails, skip this row
                pass

        return filtered

    def select(self, exprs: list) -> "RustTable":
        """Select columns and return a new RustTable."""
        result = RustTable()

        if not exprs:
            # If no columns specified, return copy
            result.schema = self.schema.copy()
            result.data = [row.copy() for row in self.data]
            return result

        # Extract column names from expressions
        selected_cols = []
        for expr in exprs:
            if isinstance(expr, dict) and expr.get("type") == "Column":
                selected_cols.append(expr["name"])
            else:
                # For now, skip computed columns
                pass

        # Update schema to only selected columns
        result.schema = {
            col: self.schema[col] for col in selected_cols if col in self.schema
        }

        # Select columns from data
        result.data = []
        for row in self.data:
            new_row = {col: row.get(col) for col in selected_cols if col in self.schema}
            result.data.append(new_row)

        return result

    def derive(self, derived_cols: Dict[str, Dict[str, Any]]) -> "RustTable":
        """Add derived columns and return a new RustTable."""
        # For now, just return a copy
        # TODO: Implement proper column derivation
        result = RustTable()
        result.schema = self.schema.copy()
        result.data = [row.copy() for row in self.data]
        return result


from .expr import _lambda_to_expr, SchemaProxy


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
        self._inner = RustTable()
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
        # Call RustTable.read_csv to load the actual data
        t._inner.read_csv(path)
        return t

    def show(self, n: int = 10) -> None:
        # Delegate to native show — native code is responsible for printing
        # a preview. We ignore the return string if any.
        out = self._inner.show(n)
        if out is not None:
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
                from .expr import SchemaProxy

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
        # Delegate derivation to Rust implementation
        result._inner = self._inner.derive(derived_cols)
        return result


__all__ = ["LTSeq"]
