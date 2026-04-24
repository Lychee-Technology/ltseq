"""Helper functions for LTSeq core operations."""

from typing import Any, Callable, cast

from .expr import BinOpExpr, ColumnExpr, SchemaProxy


def _extract_join_keys(
    join_fn: Callable,
    source_schema: dict[str, str],
    target_schema: dict[str, str],
    join_type: str = "inner",
) -> tuple[dict[str, Any], dict[str, Any], str]:
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


def _extract_all_equalities_from_and(expr: BinOpExpr) -> list[tuple[Any, Any]]:
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
    source_schema: dict[str, str],
    target_schema: dict[str, str],
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
    source_schema: dict[str, str],
    target_schema: dict[str, str],
) -> tuple[str, str, str]:
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
    left_name = cast(str, left_expr.name)
    right_name = cast(str, right_expr.name)
    left_in_source = left_name in source_schema
    right_in_target = right_name in target_schema

    if not left_in_source:
        # Check if reversed
        if left_name in target_schema and right_name in source_schema:
            raise ValueError(
                f"Asof join condition has reversed order. Expected: left_table.col >= right_table.col, "
                f"but got: {left_name} {expr.op} {right_name}. "
                f"Please reverse the comparison."
            )
        raise ValueError(
            f"Column '{left_name}' not found in source (left) schema. "
            f"Available columns: {list(source_schema.keys())}"
        )

    if not right_in_target:
        raise ValueError(
            f"Column '{right_name}' not found in target (right) schema. "
            f"Available columns: {list(target_schema.keys())}"
        )

    return (left_name, right_name, expr.op)
