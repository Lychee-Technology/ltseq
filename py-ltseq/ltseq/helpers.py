"""Helper functions for LTSeq core operations."""

from typing import Callable, Dict, Any
import csv

from .expr import SchemaProxy, BinOpExpr, ColumnExpr


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


def _infer_schema_from_csv(path: str, has_header: bool = True) -> Dict[str, str]:
    """
    Infer schema from CSV file by reading the header and sampling rows.

    This is a workaround until LTSeqTable properly exposes schema.
    Infers type as:
    - "int64" if all values are integers
    - "float64" if all values are floats
    - "string" otherwise

    Args:
        path: Path to CSV file
        has_header: Whether the CSV file has a header row. If False,
                   generates column names like "column_0", "column_1", etc.

    Returns:
        Dict mapping column names to inferred type strings
    """
    schema = {}

    try:
        with open(path, "r") as f:
            if has_header:
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
            else:
                # No header: read first row to determine column count
                reader = csv.reader(f)
                first_row = next(reader, None)
                if first_row is None:
                    return schema

                # Generate column names: column_1, column_2, ... (1-based to match DataFusion)
                num_cols = len(first_row)
                columns = [f"column_{i + 1}" for i in range(num_cols)]

                # Initialize schema with all columns as string
                for col in columns:
                    schema[col] = "string"

                # First row is data, so include it in type inference
                all_rows = [first_row]
                for i, row in enumerate(reader):
                    if i >= 99:  # Sample first 100 rows (99 more + 1 already read)
                        break
                    all_rows.append(row)

                # Infer types from sampled rows
                for row in all_rows:
                    for col_idx, value in enumerate(row):
                        if col_idx >= num_cols or value == "":
                            continue

                        col = columns[col_idx]
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
    join_fn: Callable,
    source_schema: Dict[str, str],
    target_schema: Dict[str, str],
    join_type: str = "inner",
) -> tuple:
    """
    Extract join key columns from a two-parameter lambda expression.

    Parses join conditions like:
        lambda orders, products: orders.product_id == products.id

    Supports composite keys:
        lambda orders, products: (orders.product_id == products.id) & (orders.year == products.year)

    Supports multiple join types (inner, left, right, full).

    Args:
        join_fn: Lambda function with two parameters (source row, target row)
                E.g., lambda o, p: o.product_id == p.id
                Or: lambda o, p: (o.id == p.id) & (o.year == p.year)
        source_schema: Dict mapping source column names to types
        target_schema: Dict mapping target column names to types
        join_type: Type of join ("inner", "left", "right", "full"). Default: "inner"

    Returns:
        Tuple of (left_key_expr, right_key_expr, join_type)

    Raises:
        TypeError: If join condition is not equality comparisons
        ValueError: If columns don't exist in respective schemas
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
    if not isinstance(expr, BinOpExpr):
        raise TypeError(
            f"Join condition must be a simple equality comparison or composite And-expression, got {type(expr).__name__}"
        )

    # Handle composite keys with And operator
    if expr.op == "And":
        # Recursively extract all equality pairs from And-expression
        all_equalities = _extract_all_equalities_from_and(expr)

        if not all_equalities:
            raise TypeError(
                "And-expression must contain at least one equality comparison"
            )

        # Validate all equalities
        for left_eq, right_eq in all_equalities:
            _validate_equality_pair(left_eq, right_eq, source_schema, target_schema)

        # For Rust compatibility, return the original And-expression
        left_key_expr = expr.serialize()
        right_key_expr = expr.serialize()
        return left_key_expr, right_key_expr, join_type

    # Handle single equality
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
                "Complex expressions are not supported."
            )

        # Validate the pair
        _validate_equality_pair(left_expr, right_expr, source_schema, target_schema)

        # Serialize the key expressions
        left_key_expr = left_expr.serialize()
        right_key_expr = right_expr.serialize()

        return left_key_expr, right_key_expr, join_type

    else:
        raise TypeError(
            f"Join condition must use equality (==), got operator: {expr.op}. "
            "Only equality joins are supported."
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
            "Complex expressions are not supported."
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


def _extract_asof_keys(
    join_fn: Callable,
    source_schema: Dict[str, str],
    target_schema: Dict[str, str],
) -> tuple:
    """
    Extract time column names and operator from an asof join condition.

    Parses inequality conditions like:
        lambda t, q: t.time >= q.time  -> ("time", "time", "Ge")
        lambda t, q: t.trade_time <= q.quote_time  -> ("trade_time", "quote_time", "Le")

    Args:
        join_fn: Lambda function with two parameters (source row, target row)
                E.g., lambda t, q: t.time >= q.time
        source_schema: Dict mapping source column names to types
        target_schema: Dict mapping target column names to types

    Returns:
        Tuple of (left_col_name, right_col_name, operator)
        where operator is one of "Ge", "Le", "Gt", "Lt"

    Raises:
        TypeError: If join condition is not an inequality comparison
        ValueError: If columns don't exist in respective schemas
    """
    # Create proxies for both tables
    source_proxy = SchemaProxy(source_schema)
    target_proxy = SchemaProxy(target_schema)

    # Call the join function to get the expression tree
    try:
        expr = join_fn(source_proxy, target_proxy)
    except Exception as e:
        raise TypeError(f"Failed to evaluate asof join condition: {e}")

    # Validate and extract key expressions
    if not isinstance(expr, BinOpExpr):
        raise TypeError(
            f"Asof join condition must be an inequality comparison, got {type(expr).__name__}"
        )

    # Check for valid inequality operators
    # Note: BinOpExpr uses "Ge", "Le", "Gt", "Lt" for >=, <=, >, <
    valid_ops = {"Ge", "Le", "Gt", "Lt"}
    if expr.op not in valid_ops:
        raise TypeError(
            f"Asof join condition must use inequality (>=, <=, >, <), got operator: {expr.op}. "
            f"Valid operators: {valid_ops}"
        )

    # Extract left and right operands
    left_expr = expr.left
    right_expr = expr.right

    # Validate operands are ColumnExpr
    if not isinstance(left_expr, ColumnExpr) or not isinstance(right_expr, ColumnExpr):
        raise TypeError(
            "Asof join condition must compare two columns directly. "
            "Complex expressions are not supported."
        )

    # Validate that left operand is from source and right from target
    left_in_source = left_expr.name in source_schema
    right_in_target = right_expr.name in target_schema

    if not left_in_source:
        # Check if reversed
        if left_expr.name in target_schema and right_expr.name in source_schema:
            raise ValueError(
                f"Asof join condition has reversed order. Expected: left_table.col >= right_table.col, "
                f"but got: {left_expr.name} {expr.op} {right_expr.name}. "
                f"Please reverse the comparison."
            )
        raise ValueError(
            f"Column '{left_expr.name}' not found in source (left) schema. "
            f"Available columns: {list(source_schema.keys())}"
        )

    if not right_in_target:
        raise ValueError(
            f"Column '{right_expr.name}' not found in target (right) schema. "
            f"Available columns: {list(target_schema.keys())}"
        )

    return (left_expr.name, right_expr.name, expr.op)
