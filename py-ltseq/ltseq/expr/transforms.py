"""AST transformation and lambda expression capturing for LTSeq."""

import ast
import inspect
from typing import Any, Dict

from .base import Expr
from .proxy import SchemaProxy


class IsNoneTransformer(ast.NodeTransformer):
    """
    Transform 'x is None' and 'x is not None' to '==' and '!=' comparisons.

    This allows filter expressions like:
        lambda r: r.col is not None
    to work by converting them to:
        lambda r: r.col != None

    which can be intercepted by Expr.__ne__().
    """

    def visit_Compare(self, node):
        """
        Transform Compare nodes that use 'is' or 'is not' with None.

        Args:
            node: The Compare AST node

        Returns:
            Modified Compare node or original if no 'is' comparisons found
        """
        # First, recursively visit child nodes
        node = self.generic_visit(node)

        # Check if any of the operators are 'is' or 'is not'
        new_ops = []
        new_comparators = []

        for i, (op, comparator) in enumerate(zip(node.ops, node.comparators)):  # type: ignore
            # Check if this is comparing with None
            is_none_comparison = (
                isinstance(comparator, ast.Constant) and comparator.value is None
            )

            if isinstance(op, ast.Is) and is_none_comparison:
                # Transform 'x is None' to 'x == None'
                new_ops.append(ast.Eq())
                new_comparators.append(comparator)
            elif isinstance(op, ast.IsNot) and is_none_comparison:
                # Transform 'x is not None' to 'x != None'
                new_ops.append(ast.NotEq())
                new_comparators.append(comparator)
            else:
                new_ops.append(op)
                new_comparators.append(comparator)

        node.ops = new_ops  # type: ignore
        node.comparators = new_comparators  # type: ignore
        return node


def _transform_lambda_for_none_checks(fn):
    """
    Transform lambda functions to replace 'is None' and 'is not None' with comparisons.

    This allows expressions like:
        lambda r: r.col is not None
    to work by converting them to:
        lambda r: r.col != None

    Args:
        fn: The lambda function to transform

    Returns:
        A new lambda function with transformed AST, or original if no transformation needed
    """
    try:
        # Get the source code of the lambda
        source = inspect.getsource(fn).strip()

        # If source ends with a comma or other characters, clean it up
        # Find the lambda expression
        if "lambda" not in source:
            return fn

        # Skip transformation if source doesn't contain 'is' comparisons
        # This avoids issues with multiple lambdas on the same line
        if " is " not in source and " is not " not in source:
            return fn

        # Check if there are multiple lambdas in the source
        # If so, skip transformation to avoid picking the wrong one
        if source.count("lambda") > 1:
            return fn

        # Extract just the lambda expression
        lambda_start = source.find("lambda")
        if lambda_start == -1:
            return fn

        # Find the end of the lambda expression by counting parentheses and brackets
        start_pos = lambda_start
        end_pos = lambda_start + len("lambda")
        paren_count = 0
        bracket_count = 0
        brace_count = 0
        in_string = False
        string_char = None

        for i in range(end_pos, len(source)):
            c = source[i]

            # Handle strings
            if c in ('"', "'") and (i == 0 or source[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = c
                elif c == string_char:
                    in_string = False
                continue

            if in_string:
                continue

            if c == "(":
                paren_count += 1
            elif c == ")":
                if paren_count == 0:
                    # This is the end of the lambda expression
                    end_pos = i
                    break
                paren_count -= 1
            elif c == "[":
                bracket_count += 1
            elif c == "]":
                bracket_count -= 1
            elif c == "{":
                brace_count += 1
            elif c == "}":
                brace_count -= 1
            elif (
                c in (",", ";")
                and paren_count == 0
                and bracket_count == 0
                and brace_count == 0
            ):
                # End of lambda at comma/semicolon
                end_pos = i
                break

            end_pos = i + 1

        lambda_source = source[lambda_start:end_pos].strip()

        # Parse it as an expression
        tree = ast.parse(lambda_source, mode="eval")

        # Transform the tree
        transformer = IsNoneTransformer()
        new_tree = transformer.visit(tree)

        # Fix missing locations
        ast.fix_missing_locations(new_tree)

        # Compile and evaluate to get a new lambda
        code = compile(new_tree, "<lambda>", "eval")
        new_fn = eval(code)

        return new_fn
    except Exception:
        # If transformation fails for any reason, return original
        return fn


def _lambda_to_expr(fn, schema: Dict[str, str]) -> Dict[str, Any]:
    """
    Execute a lambda with a SchemaProxy to capture its expression tree.

    This is the core function that intercepts Python lambdas and converts them
    to serializable expression dicts without executing any Python logic.

    Args:
        fn: Lambda function, e.g., lambda r: r.age > 18 or lambda r: {"col": r.age}
        schema: Dict mapping column name -> type string

    Returns:
        Serialized expression dict, ready for Rust deserialization
        Or for dict lambdas: {"type": "Dict", "keys": [...], "values": [...]}

    Raises:
        TypeError: If lambda doesn't return an Expr or dict
        AttributeError: If lambda references a non-existent column

    Example:
        >>> schema = {"age": "int64", "name": "string"}
        >>> expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)
        >>> expr_dict["type"]
        'BinOp'
        >>> expr_dict["op"]
        'Gt'
        >>> # Or for dict returns:
        >>> expr_dict = _lambda_to_expr(lambda r: {"adult": r.age > 18}, schema)
        >>> expr_dict["type"]
        'Dict'
    """
    proxy = SchemaProxy(schema)

    # Transform the lambda to replace 'is None' and 'is not None' with comparisons
    fn = _transform_lambda_for_none_checks(fn)

    result = fn(proxy)

    if isinstance(result, dict):
        # Handle dict returns: {"col_name": Expr, "col_name2": Expr}
        keys = []
        values = []
        for key, value in result.items():
            if not isinstance(key, str):
                raise TypeError(f"Dict keys must be strings, got {type(key).__name__}")
            if not isinstance(value, Expr):
                raise TypeError(
                    f"Dict values must be Expr objects, got {type(value).__name__} for key '{key}'"
                )
            keys.append({"type": "Literal", "value": key, "dtype": "String"})
            values.append(value.serialize())

        return {"type": "Dict", "keys": keys, "values": values}
    elif isinstance(result, Expr):
        # Handle Expr returns: lambda r: r.age > 18
        return result.serialize()
    else:
        raise TypeError(
            f"Lambda must return an Expr or dict, got {type(result).__name__}. "
            "Did you forget to use the 'r' parameter?"
        )
