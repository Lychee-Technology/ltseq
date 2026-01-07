"""
Expression system for LTSeq: Captures Python lambdas as serializable AST trees.

This module provides a set of classes that intercept Python operators and method
calls without executing them. Instead, it builds a tree representation that Phase 4
will transpile to DataFusion logical plans.

Core Classes:
  - Expr: Abstract base class for all expressions
  - ColumnExpr: Column reference (e.g., r.age)
  - LiteralExpr: Constant value (e.g., 42, "hello")
  - BinOpExpr: Binary operation (e.g., r.age + 5, r.price > 10)
  - UnaryOpExpr: Unary operation (e.g., ~r.flag)
  - CallExpr: Function/method call (e.g., r.col.shift(1), r.col.rolling(3).mean())
  - SchemaProxy: Row proxy for capturing expressions in lambdas

Example:
  >>> schema = {"age": "int64", "name": "string"}
  >>> r = SchemaProxy(schema)
  >>> expr = r.age > 18
  >>> expr.serialize()
  {'type': 'BinOp', 'op': 'Gt', 'left': {'type': 'Column', 'name': 'age'}, ...}
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import ast
import inspect


class Expr(ABC):
    """
    Abstract base class for all expression types.

    Implements magic methods (__add__, __gt__, etc.) that return new Expr objects
    instead of evaluating. This allows building expression trees from Python code.
    """

    @abstractmethod
    def serialize(self) -> Dict[str, Any]:
        """
        Convert this expression to a serializable nested dict.

        Returns:
            A dict with at least a 'type' key, ready for Rust deserialization.
        """
        pass

    # Arithmetic operators
    def __add__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Addition operator: expr + other"""
        return BinOpExpr("Add", self, self._coerce(other))

    def __sub__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Subtraction operator: expr - other"""
        return BinOpExpr("Sub", self, self._coerce(other))

    def __mul__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Multiplication operator: expr * other"""
        return BinOpExpr("Mul", self, self._coerce(other))

    def __truediv__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Division operator: expr / other"""
        return BinOpExpr("Div", self, self._coerce(other))

    def __floordiv__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Floor division operator: expr // other"""
        return BinOpExpr("FloorDiv", self, self._coerce(other))

    def __mod__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Modulo operator: expr % other"""
        return BinOpExpr("Mod", self, self._coerce(other))

    # Comparison operators
    # Note: These override object.__eq__ and __ne__, intentionally returning Expr instead of bool
    def __eq__(self, other: Union["Expr", Any]):  # type: ignore
        """Equality operator: expr == other"""
        return BinOpExpr("Eq", self, self._coerce(other))

    def __ne__(self, other: Union["Expr", Any]):  # type: ignore
        """Inequality operator: expr != other"""
        return BinOpExpr("Ne", self, self._coerce(other))

    def __lt__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Less than operator: expr < other"""
        return BinOpExpr("Lt", self, self._coerce(other))

    def __le__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Less than or equal operator: expr <= other"""
        return BinOpExpr("Le", self, self._coerce(other))

    def __gt__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Greater than operator: expr > other"""
        return BinOpExpr("Gt", self, self._coerce(other))

    def __ge__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Greater than or equal operator: expr >= other"""
        return BinOpExpr("Ge", self, self._coerce(other))

    # Logical operators
    def __and__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Logical AND operator: expr & other"""
        return BinOpExpr("And", self, self._coerce(other))

    def __or__(self, other: Union["Expr", Any]) -> "BinOpExpr":
        """Logical OR operator: expr | other"""
        return BinOpExpr("Or", self, self._coerce(other))

    def __invert__(self) -> "UnaryOpExpr":
        """Logical NOT operator: ~expr"""
        return UnaryOpExpr("Not", self)

    def __abs__(self) -> "CallExpr":
        """Absolute value operator: abs(expr)"""
        return CallExpr("abs", (self,), {}, on=None)

    # Right-hand operators (for reversed operations like 5 + r.col)
    def __radd__(self, other: Any) -> "BinOpExpr":
        """Right addition: other + expr"""
        return BinOpExpr("Add", self._coerce(other), self)

    def __rsub__(self, other: Any) -> "BinOpExpr":
        """Right subtraction: other - expr"""
        return BinOpExpr("Sub", self._coerce(other), self)

    def __rmul__(self, other: Any) -> "BinOpExpr":
        """Right multiplication: other * expr"""
        return BinOpExpr("Mul", self._coerce(other), self)

    def __rtruediv__(self, other: Any) -> "BinOpExpr":
        """Right division: other / expr"""
        return BinOpExpr("Div", self._coerce(other), self)

    def __rfloordiv__(self, other: Any) -> "BinOpExpr":
        """Right floor division: other // expr"""
        return BinOpExpr("FloorDiv", self._coerce(other), self)

    def __rmod__(self, other: Any) -> "BinOpExpr":
        """Right modulo: other % expr"""
        return BinOpExpr("Mod", self._coerce(other), self)

    @staticmethod
    def _coerce(value: Any) -> "Expr":
        """
        Convert Python literals to LiteralExpr.

        Args:
            value: A Python value or Expr

        Returns:
            An Expr (unchanged if already Expr, wrapped in LiteralExpr otherwise)
        """
        if isinstance(value, Expr):
            return value
        return LiteralExpr(value)


class ColumnExpr(Expr):
    """
    Represents a column reference, e.g., r.age in a lambda.

    When accessed as r.col.shift(1), __getattr__ returns a callable that
    creates a CallExpr when invoked.

    Attributes:
        name (str): The column name
    """

    def __init__(self, name: str):
        """
        Initialize a ColumnExpr.

        Args:
            name: Column name (e.g., "age", "price")
        """
        self.name = name

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict.

        Returns:
            {"type": "Column", "name": self.name}
        """
        return {"type": "Column", "name": self.name}

    def __getattr__(self, method_name: str):
        """
        Handle method calls like r.col.shift(1), r.col.rolling(3), etc.

        Returns a callable that, when invoked with arguments, creates a CallExpr.

        Args:
            method_name: Method to call (e.g., "shift", "rolling", "contains")

        Returns:
            A callable that accepts (*args, **kwargs) and returns CallExpr

        Raises:
            AttributeError: If method_name starts with underscore (private)
        """
        if method_name.startswith("_"):
            # Avoid issues with __dict__, __class__, etc.
            raise AttributeError(f"No attribute {method_name}")

        def method_call(*args, **kwargs):
            """Create a CallExpr when the method is invoked."""
            return CallExpr(method_name, args, kwargs, on=self)

        return method_call


class LiteralExpr(Expr):
    """
    Represents a constant value: int, float, str, bool, None.

    Includes type inference to determine the appropriate Arrow/DataFusion type.

    Attributes:
        value: The Python value
    """

    def __init__(self, value: Any):
        """
        Initialize a LiteralExpr.

        Args:
            value: The constant value (int, float, str, bool, None, etc.)
        """
        self.value = value

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with inferred dtype.

        Returns:
            {
                "type": "Literal",
                "value": self.value,
                "dtype": inferred_type_string
            }
        """
        return {"type": "Literal", "value": self.value, "dtype": self._infer_dtype()}

    def _infer_dtype(self) -> str:
        """
        Infer Arrow/DataFusion type from Python value.

        Returns:
            A type string: "Boolean", "Int64", "Float64", "String", "Null"
        """
        if isinstance(self.value, bool):
            # Must check bool before int (bool is subclass of int)
            return "Boolean"
        elif isinstance(self.value, int):
            return "Int64"
        elif isinstance(self.value, float):
            return "Float64"
        elif isinstance(self.value, str):
            return "String"
        elif self.value is None:
            return "Null"
        else:
            # Fallback: serialize as string
            return "String"


class BinOpExpr(Expr):
    """
    Represents a binary operation: +, -, >, <, ==, &, |, etc.

    Attributes:
        op (str): Operation name ("Add", "Gt", "And", etc.)
        left (Expr): Left operand
        right (Expr): Right operand
    """

    def __init__(self, op: str, left: Expr, right: Expr):
        """
        Initialize a BinOpExpr.

        Args:
            op: Operation name (e.g., "Add", "Gt", "And")
            left: Left operand (Expr)
            right: Right operand (Expr)
        """
        self.op = op
        self.left = left
        self.right = right

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of operands.

        Returns:
            {
                "type": "BinOp",
                "op": self.op,
                "left": self.left.serialize(),
                "right": self.right.serialize()
            }
        """
        return {
            "type": "BinOp",
            "op": self.op,
            "left": self.left.serialize(),
            "right": self.right.serialize(),
        }


class UnaryOpExpr(Expr):
    """
    Represents a unary operation: NOT (~), etc.

    Attributes:
        op (str): Operation name ("Not", etc.)
        operand (Expr): The operand
    """

    def __init__(self, op: str, operand: Expr):
        """
        Initialize a UnaryOpExpr.

        Args:
            op: Operation name (e.g., "Not")
            operand: The operand (Expr)
        """
        self.op = op
        self.operand = operand

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of operand.

        Returns:
            {
                "type": "UnaryOp",
                "op": self.op,
                "operand": self.operand.serialize()
            }
        """
        return {"type": "UnaryOp", "op": self.op, "operand": self.operand.serialize()}


class CallExpr(Expr):
    """
    Represents a function/method call: r.col.shift(1), r.col.rolling(3).sum(), etc.

    Supports both method calls (where 'on' is not None) and standalone functions.
    Supports chaining via __getattr__ returning a callable.

    Attributes:
        func (str): Function/method name
        args (tuple): Positional arguments
        kwargs (dict): Keyword arguments
        on (Expr or None): Object the method is called on (None for functions)
    """

    def __init__(
        self,
        func: str,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        on: Optional[Expr] = None,
    ):
        """
        Initialize a CallExpr.

        Args:
            func: Function/method name (e.g., "shift", "rolling", "sum")
            args: Positional arguments (tuple, may contain Exprs)
            kwargs: Keyword arguments (dict, may contain Exprs)
            on: Object the method is called on, or None for functions
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.on = on

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict with recursive serialization of args/kwargs/on.

        Args are serialized if they're Exprs; literals are converted to LiteralExpr first.

        Returns:
            {
                "type": "Call",
                "func": self.func,
                "args": [...],  # serialized args
                "kwargs": {...},  # serialized kwargs
                "on": serialized_on or None
            }
        """

        def serialize_value(v):
            """Serialize a value: Expr -> dict, literal -> LiteralExpr -> dict"""
            if isinstance(v, Expr):
                return v.serialize()
            # Convert literal to LiteralExpr for serialization
            return LiteralExpr(v).serialize()

        return {
            "type": "Call",
            "func": self.func,
            "args": [serialize_value(arg) for arg in self.args],
            "kwargs": {k: serialize_value(v) for k, v in self.kwargs.items()},
            "on": self.on.serialize() if self.on else None,
        }

    def __getattr__(self, method_name: str):
        """
        Allow chaining: r.col.rolling(3).mean().

        This CallExpr becomes the 'on' of the next CallExpr.

        Args:
            method_name: Method to call on the result of this call

        Returns:
            A callable that accepts (*args, **kwargs) and returns CallExpr

        Raises:
            AttributeError: If method_name starts with underscore
        """
        if method_name.startswith("_"):
            raise AttributeError(f"No attribute {method_name}")

        def chained_call(*args, **kwargs):
            """Create a CallExpr with self as the 'on' target."""
            return CallExpr(method_name, args, kwargs, on=self)

        return chained_call


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
            ) or (isinstance(comparator, ast.NameConstant) and comparator.value is None)

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


class SchemaProxy:
    """
    Represents the row proxy ('r') passed into lambda functions.

    When user accesses r.age, this returns a ColumnExpr("age").
    Validates that the column exists in the schema, raising AttributeError if not.

    This allows lambdas like: lambda r: r.age > 18 to build an Expr tree
    instead of executing Python code.

    Attributes:
        _schema (dict): Mapping of column name -> type string
    """

    def __init__(self, schema: Dict[str, str]):
        """
        Initialize a SchemaProxy.

        Args:
            schema: Dict mapping column name -> type string
                    E.g., {"age": "int64", "name": "string", "price": "float64"}
        """
        self._schema = schema

    def __getattr__(self, name: str) -> ColumnExpr:
        """
        Return a ColumnExpr for the given column name.

        Validates that the column exists in the schema.

        Args:
            name: Column name

        Returns:
            ColumnExpr(name) if column exists

        Raises:
            AttributeError: If column not in schema or name is private
        """
        if name.startswith("_"):
            # Avoid issues with internal attributes like _schema
            raise AttributeError(f"No attribute {name}")

        if name not in self._schema:
            raise AttributeError(
                f"Column '{name}' not found in schema. "
                f"Available columns: {list(self._schema.keys())}"
            )

        return ColumnExpr(name)

    def get_schema(self) -> Dict[str, str]:
        """
        Return a copy of the underlying schema.

        Returns:
            A dict copy of {column_name -> type_string}
        """
        return self._schema.copy()


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

        # Extract just the lambda expression
        lambda_start = source.find("lambda")
        if lambda_start == -1:
            return fn

        lambda_source = source[lambda_start:].split(",")[0].split(")")[0]
        if lambda_source.endswith(")"):
            # Handle case where there's a closing paren after lambda
            # Need to find the matching pair
            open_count = 0
            end_idx = lambda_start + 6  # Skip 'lambda'
            for i, c in enumerate(source[lambda_start + 6 :]):
                if c == "(":
                    open_count += 1
                elif c == ")":
                    if open_count == 0:
                        end_idx = lambda_start + 6 + i
                        break
                    open_count -= 1
            lambda_source = source[lambda_start:end_idx]
        else:
            lambda_source = source[lambda_start:]

        print(f"DEBUG: Extracted lambda source: {lambda_source}")

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

        print(f"DEBUG: Successfully created transformed lambda")
        return new_fn
    except Exception as e:
        # If transformation fails for any reason, return original
        print(f"DEBUG: Transformation failed: {e}")
        import traceback

        traceback.print_exc()
        return fn

        # Extract just the lambda expression
        lambda_start = source.find("lambda")
        if lambda_start == -1:
            return fn

        lambda_source = source[lambda_start:].split(",")[0].split(")")[0]
        if lambda_source.endswith(")"):
            # Handle case where there's a closing paren after lambda
            # Need to find the matching pair
            open_count = 0
            end_idx = lambda_start + 6  # Skip 'lambda'
            for i, c in enumerate(source[lambda_start + 6 :]):
                if c == "(":
                    open_count += 1
                elif c == ")":
                    if open_count == 0:
                        end_idx = lambda_start + 6 + i
                        break
                    open_count -= 1
            lambda_source = source[lambda_start:end_idx]
        else:
            lambda_source = source[lambda_start:]

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
