"""AST transformation and lambda expression capturing for LTSeq."""

import ast
import copy
import inspect
import types
from typing import Any, Callable, cast

from .base import Expr
from .proxy import SchemaProxy


_NULL_CHECK_HELPER = "__ltseq_null_check__"


def _null_check(value: Any, negate: bool) -> Any:
    """Runtime target of a rewritten ``x is None`` / ``x is not None``.

    Expression operands become explicit null predicates; anything else keeps
    plain Python identity semantics, so captured Python values such as
    ``lambda r: r.a > th if th is not None else r.a`` behave as written.
    """
    if isinstance(value, Expr):
        return value.is_not_null() if negate else value.is_null()
    return (value is not None) if negate else (value is None)


def _is_none_constant(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _none_check_operand(node: ast.Compare) -> tuple[ast.expr, bool] | None:
    """Return ``(operand, negate)`` if ``node`` is ``x is None`` / ``x is not None``
    (or the reversed spelling), else ``None``. Chained comparisons are not rewritten."""
    if len(node.ops) != 1 or not isinstance(node.ops[0], (ast.Is, ast.IsNot)):
        return None
    left, right = node.left, node.comparators[0]
    if _is_none_constant(right) and not _is_none_constant(left):
        operand = left
    elif _is_none_constant(left) and not _is_none_constant(right):
        operand = right
    else:
        return None
    return operand, isinstance(node.ops[0], ast.IsNot)


def _contains_none_check(node: ast.AST) -> bool:
    return any(
        isinstance(sub, ast.Compare) and _none_check_operand(sub) is not None
        for sub in ast.walk(node)
    )


class _NoneCheckRewriter(ast.NodeTransformer):
    """Rewrite ``x is None`` / ``x is not None`` into ``__ltseq_null_check__(x, negate)``.

    ``is`` cannot be overloaded in Python, so the comparison is redirected to
    :func:`_null_check`, which yields ``x.is_null()`` / ``x.is_not_null()`` for
    expressions and plain identity checks for everything else.
    """

    def visit_Compare(self, node: ast.Compare) -> ast.expr:
        node = cast(ast.Compare, self.generic_visit(node))
        match = _none_check_operand(node)
        if match is None:
            return node
        operand, negate = match
        call = ast.Call(
            func=ast.Name(id=_NULL_CHECK_HELPER, ctx=ast.Load()),
            args=[operand, ast.Constant(value=negate)],
            keywords=[],
        )
        return ast.copy_location(call, node)


# filename -> (line list handed out by linecache, lineno -> lambdas that use `is None`)
_ModuleIndex = dict[int, list[ast.Lambda]]
_module_index_cache: dict[str, tuple[list[str], _ModuleIndex]] = {}


def _module_none_check_lambdas(fn: types.FunctionType) -> _ModuleIndex | None:
    """Parse the module that defines ``fn`` (cached) and index, by line, the
    lambdas whose body contains an ``is None`` / ``is not None`` comparison.

    Returns ``None`` when the source is unavailable (REPL, ``eval`` strings) or
    does not parse as the running interpreter's Python.
    """
    try:
        lines, _ = inspect.findsource(fn)
    except (OSError, TypeError):
        return None
    filename = fn.__code__.co_filename
    cached = _module_index_cache.get(filename)
    if cached is not None and cached[0] is lines:
        return cached[1]
    try:
        module = ast.parse("".join(lines), filename)
    except (SyntaxError, ValueError):
        return None
    index: _ModuleIndex = {}
    for node in ast.walk(module):
        if isinstance(node, ast.Lambda) and _contains_none_check(node):
            index.setdefault(node.lineno, []).append(node)
    _module_index_cache[filename] = (lines, index)
    return index


def _compile_lambda_in_scope(
    node: ast.Lambda, scope_names: tuple[str, ...], filename: str
) -> types.CodeType:
    """Compile ``node`` as if it were nested in a function whose locals are
    ``scope_names`` and return the lambda's own code object.

    Names in ``scope_names`` that the lambda references compile to closure
    loads exactly as they did in the original function; every other name stays
    a global lookup. The node is not mutated.
    """
    wrapper = ast.Lambda(
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg=name) for name in scope_names],
            vararg=None,
            kwonlyargs=[],
            kw_defaults=[],
            kwarg=None,
            defaults=[],
        ),
        body=node,
    )
    ast.copy_location(wrapper, node)
    expression = ast.fix_missing_locations(ast.Expression(body=wrapper))
    wrapper_code = next(
        const
        for const in compile(expression, filename, "eval").co_consts
        if isinstance(const, types.CodeType)
    )
    return next(
        const for const in wrapper_code.co_consts if isinstance(const, types.CodeType)
    )


def _locate_lambda(fn: types.FunctionType, index: _ModuleIndex) -> ast.Lambda | None:
    """Find the AST node ``fn`` was compiled from, by compiling each lambda on
    its first line (with the same free variables) and comparing code objects.

    Byte-for-byte identity is what makes this independent of how many lambdas
    share a line, of string literals on the line, and of line continuations.
    """
    code = fn.__code__
    for candidate in index.get(code.co_firstlineno, ()):
        if _compile_lambda_in_scope(candidate, code.co_freevars, code.co_filename) == code:
            return candidate
    return None


def _transform_lambda_for_none_checks(fn: Callable) -> Callable:
    """Return ``fn`` with ``is None`` / ``is not None`` rewritten to explicit
    null checks, or ``fn`` itself when nothing needs rewriting.

    Only lambdas are rewritten: the lambda is located in its module's AST by
    compiled identity, rewritten by :class:`_NoneCheckRewriter`, recompiled
    with the same free variables, and rebuilt on the original ``__globals__``
    and the original closure cells (matched by name, so live bindings are
    preserved). When the source cannot be inspected, ``fn`` is returned
    unchanged and :func:`_lambda_to_expr` reports how to spell the check.
    """
    if not isinstance(fn, types.FunctionType) or fn.__code__.co_name != "<lambda>":
        return fn
    index = _module_none_check_lambdas(fn)
    if not index:
        return fn
    node = _locate_lambda(fn, index)
    if node is None:
        return fn

    original = fn.__code__
    rewritten = _NoneCheckRewriter().visit(copy.deepcopy(node))
    new_code = _compile_lambda_in_scope(
        rewritten, original.co_freevars + (_NULL_CHECK_HELPER,), original.co_filename
    )

    cells = dict(zip(original.co_freevars, fn.__closure__ or ()))
    cells[_NULL_CHECK_HELPER] = types.CellType(_null_check)
    closure = tuple(cells[name] for name in new_code.co_freevars)

    new_fn = types.FunctionType(
        new_code, fn.__globals__, fn.__name__, fn.__defaults__, closure
    )
    new_fn.__kwdefaults__ = fn.__kwdefaults__
    new_fn.__qualname__ = fn.__qualname__
    return new_fn


def _lambda_to_expr(fn: Callable, schema: dict[str, str]) -> dict[str, Any]:
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

    # `is` cannot be overloaded: rewrite `x is None` / `x is not None` in the
    # lambda into explicit null checks before running it against the proxy.
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
        # Check if this might be an 'is None' / 'is not None' issue
        hint = ""
        if result is True or result is False:
            hint = (
                "\n\nHint: If you're using 'is None' or 'is not None', use the "
                "is_null() or is_not_null() methods instead:\n"
                "  - r.col.is_null()      instead of  r.col is None\n"
                "  - r.col.is_not_null()  instead of  r.col is not None\n"
                "LTSeq rewrites 'is None' checks inside lambdas automatically "
                "when their source is available; the methods are required in "
                "a REPL or exec/eval string and in plain def functions."
            )
        raise TypeError(
            f"Lambda must return an Expr or dict, got {type(result).__name__}. "
            f"Did you forget to use the 'r' parameter?{hint}"
        )
